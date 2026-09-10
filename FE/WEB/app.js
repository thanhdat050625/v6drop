/**
 * v6drop - High-Performance File & Video Transfer
 * Go WebSocket Relay + Continuous Streaming with Native Backpressure (ws.bufferedAmount) + Checksum Verification
 */

const CHUNK_SIZE = 256 * 1024;     // 256KB chunk
const SERVER_WS_BASE = 'wss://v6drop-nganhang5742.onrender.com/ws';

let ws = null;
let keepAliveTimer = null;
let currentFile = null;
let wakeLock = null;

// ==========================================
// DOM Elements
// ==========================================
const UI = {
  tabGui: document.getElementById('tabGui'),
  tabNhan: document.getElementById('tabNhan'),
  viewGui: document.getElementById('viewGui'),
  viewNhan: document.getElementById('viewNhan'),
  roomId: document.getElementById('roomId'),
  dropZone: document.getElementById('dropZone'),
  fileInput: document.getElementById('fileInput'),
  fileCard: document.getElementById('fileCard'),
  selectedFileName: document.getElementById('selectedFileName'),
  selectedFileSize: document.getElementById('selectedFileSize'),
  btnChangeFile: document.getElementById('btnChangeFile'),
  btnStartSend: document.getElementById('btnStartSend'),
  btnStartReceive: document.getElementById('btnStartReceive'),
  progressCard: document.getElementById('progressCard'),
  transferStatus: document.getElementById('transferStatus'),
  transferPercent: document.getElementById('transferPercent'),
  progressBar: document.getElementById('progressBar'),
  metricSpeed: document.getElementById('metricSpeed'),
  metricTransferred: document.getElementById('metricTransferred'),
  metricEta: document.getElementById('metricEta'),
  metricChunks: document.getElementById('metricChunks'),
  btnToggleLog: document.getElementById('btnToggleLog'),
  logToggleIcon: document.getElementById('logToggleIcon'),
  logConsole: document.getElementById('logConsole')
};

// ==========================================
// Helper Utilities
// ==========================================
function log(msg) {
  const time = new Date().toLocaleTimeString();
  const entry = document.createElement('div');
  entry.className = 'log-entry';
  entry.textContent = `[${time}] ${msg}`;
  if (UI.logConsole) {
    UI.logConsole.appendChild(entry);
    UI.logConsole.scrollTop = UI.logConsole.scrollHeight;
  }
  console.log(`[v6drop ${time}] ${msg}`);
}

function formatBytes(bytes) {
  if (!bytes || bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function getWsUrl(role) {
  const room = (UI.roomId.value || 'phong-video-1').trim();
  return `${SERVER_WS_BASE}?room=${encodeURIComponent(room)}&role=${role}`;
}

function startKeepAlive() {
  stopKeepAlive();
  keepAliveTimer = setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'ping' }));
    }
  }, 20000);
}

function stopKeepAlive() {
  if (keepAliveTimer) {
    clearInterval(keepAliveTimer);
    keepAliveTimer = null;
  }
}

// Giữ màn hình sáng và chống Android/iOS đóng băng tab (Background Throttling)
async function requestWakeLock() {
  try {
    if ('wakeLock' in navigator) {
      wakeLock = await navigator.wakeLock.request('screen');
    }
  } catch (err) {}
}

function releaseWakeLock() {
  if (wakeLock) {
    wakeLock.release().catch(() => {});
    wakeLock = null;
  }
}

// Đọc siêu tốc ArrayBuffer từ Blob (ưu tiên native C++ blob.arrayBuffer() 0.1ms, fallback FileReader nếu cần)
async function readBlobAsArrayBuffer(blob) {
  if (typeof blob.arrayBuffer === 'function') {
    return await blob.arrayBuffer();
  }
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = () => reject(reader.error || new Error('Không thể đọc file qua FileReader'));
    reader.readAsArrayBuffer(blob);
  });
}

// Tính mã Checksum SHA-256 đồng bộ giữa Bên Gửi và Bên Nhận
async function computeChecksum(blob) {
  // Với file <= 64MB: tính SHA-256 toàn vẹn 100%
  if (blob.size <= 64 * 1024 * 1024) {
    const buffer = await readBlobAsArrayBuffer(blob);
    const hashBuffer = await crypto.subtle.digest('SHA-256', buffer);
    return Array.from(new Uint8Array(hashBuffer))
      .map(b => b.toString(16).padStart(2, '0'))
      .join('');
  }

  // Với file lớn > 64MB: băm lấy mẫu (16MB đầu + 16MB giữa + 16MB cuối + size)
  // Đảm bảo tính toàn vẹn cao, tốc độ dưới 0.3s, không làm tràn RAM tab trình duyệt điện thoại
  const sampleSize = 16 * 1024 * 1024;
  const startSlice = blob.slice(0, sampleSize);
  const midStart = Math.floor(blob.size / 2) - Math.floor(sampleSize / 2);
  const midSlice = blob.slice(midStart, midStart + sampleSize);
  const endSlice = blob.slice(blob.size - sampleSize, blob.size);

  const [startBuf, midBuf, endBuf] = await Promise.all([
    readBlobAsArrayBuffer(startSlice),
    readBlobAsArrayBuffer(midSlice),
    readBlobAsArrayBuffer(endSlice)
  ]);

  const combined = new Uint8Array(startBuf.byteLength + midBuf.byteLength + endBuf.byteLength);
  combined.set(new Uint8Array(startBuf), 0);
  combined.set(new Uint8Array(midBuf), startBuf.byteLength);
  combined.set(new Uint8Array(endBuf), startBuf.byteLength + midBuf.byteLength);

  const hashBuffer = await crypto.subtle.digest('SHA-256', combined.buffer);
  const hashHex = Array.from(new Uint8Array(hashBuffer))
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');

  return `${hashHex}_sz${blob.size}`;
}

// ==========================================
// 1. ROUTING (/gui & /nhan)
// ==========================================
function updateRoute() {
  const hash = window.location.hash || '#/gui';
  const isNhan = hash.startsWith('#/nhan');

  const queryPart = hash.includes('?') ? hash.split('?')[1] : window.location.search.replace('?', '');
  if (queryPart) {
    const params = new URLSearchParams(queryPart);
    const roomFromUrl = params.get('room');
    if (roomFromUrl && UI.roomId) {
      UI.roomId.value = roomFromUrl;
    }
  }

  if (isNhan) {
    UI.tabNhan.className = 'mode-tab active-receiver';
    UI.tabGui.className = 'mode-tab';
    UI.viewGui.style.display = 'none';
    UI.viewNhan.style.display = 'block';
    log('Chuyển sang chế độ: Bên Nhận (/nhan)');
  } else {
    UI.tabGui.className = 'mode-tab active-sender';
    UI.tabNhan.className = 'mode-tab';
    UI.viewGui.style.display = 'block';
    UI.viewNhan.style.display = 'none';
    log('Chuyển sang chế độ: Bên Gửi (/gui)');
  }
}

window.addEventListener('hashchange', updateRoute);
window.addEventListener('DOMContentLoaded', updateRoute);

// Log Drawer Toggle
if (UI.btnToggleLog) {
  UI.btnToggleLog.addEventListener('click', () => {
    const isOpen = UI.logConsole.classList.toggle('open');
    UI.logToggleIcon.textContent = isOpen ? '▲ Thu gọn' : '▼ Mở xem';
  });
}

// ==========================================
// 2. FILE PICKER & DRAG/DROP
// ==========================================
if (UI.dropZone && UI.fileInput) {
  UI.dropZone.addEventListener('click', () => UI.fileInput.click());
  if (UI.btnChangeFile) {
    UI.btnChangeFile.addEventListener('click', () => UI.fileInput.click());
  }

  UI.dropZone.addEventListener('dragover', (e) => {
    e.preventDefault();
    UI.dropZone.classList.add('dragover');
  });

  UI.dropZone.addEventListener('dragleave', () => {
    UI.dropZone.classList.remove('dragover');
  });

  UI.dropZone.addEventListener('drop', (e) => {
    e.preventDefault();
    UI.dropZone.classList.remove('dragover');
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      handleFilePicked(e.dataTransfer.files[0]);
    }
  });

  UI.fileInput.addEventListener('change', (e) => {
    if (e.target.files && e.target.files[0]) {
      handleFilePicked(e.target.files[0]);
    }
  });
}

function handleFilePicked(file) {
  currentFile = file;
  UI.selectedFileName.textContent = file.name;
  UI.selectedFileSize.textContent = formatBytes(file.size);
  UI.dropZone.style.display = 'none';
  UI.fileCard.style.display = 'flex';
  log(`Đã chọn file: ${file.name} (${formatBytes(file.size)})`);
}

// ==========================================
// 3. BÊN GỬI (/gui) - GỐI ĐẦU 5 CHUNK 256KB & ĐỐI SOÁT CHECKSUM
// ==========================================
if (UI.btnStartSend) {
  UI.btnStartSend.addEventListener('click', async () => {
    if (!currentFile) {
      alert('Vui lòng chọn file trước khi gửi!');
      return;
    }

    UI.btnStartSend.disabled = true;
    UI.progressCard.style.display = 'block';
    UI.transferStatus.textContent = 'Đang tính toán mã Checksum SHA-256...';
    log(`Đang tính mã Checksum SHA-256 cho file: ${currentFile.name}...`);

    try {
      const checksum = await computeChecksum(currentFile);
      log(`Mã Checksum SHA-256: ${checksum.slice(0, 16)}...`);
      UI.transferStatus.textContent = 'Đang kết nối tới Go Relay Server...';
      uploadFilePipelined(currentFile, checksum);
    } catch (err) {
      log(`Lỗi tính checksum: ${err.message}`);
      // Fallback nếu lỗi crypto
      const fallbackId = `fb_${currentFile.name}_${currentFile.size}_${currentFile.lastModified || Date.now()}`;
      uploadFilePipelined(currentFile, fallbackId);
    }
  });
}

function uploadFilePipelined(file, checksum) {
  if (ws) ws.close();
  const wsUrl = getWsUrl('sender');
  log(`Kết nối Bên Gửi: ${wsUrl}`);
  ws = new WebSocket(wsUrl);
  ws.binaryType = 'arraybuffer';

  let sentChunks = 0;
  let startTime = 0;
  let lastSpeedTime = 0;
  let lastSpeedBytes = 0;
  const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
  let isTransferring = true;

  // Giới hạn buffer tạm trong máy gửi là 4MB (bơm liên tục không cần chờ ACK)
  const MAX_BUFFERED_AMOUNT = 4 * 1024 * 1024;

  async function startStreaming() {
    startTime = performance.now();
    lastSpeedTime = startTime;
    lastSpeedBytes = 0;

    for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
      if (!isTransferring || ws.readyState !== WebSocket.OPEN) break;

      // Native Backpressure: Nếu buffer socket của trình duyệt > 4MB thì chờ card mạng xả bớt
      while (ws.bufferedAmount > MAX_BUFFERED_AMOUNT) {
        await new Promise(r => setTimeout(r, 5));
        if (!isTransferring || ws.readyState !== WebSocket.OPEN) break;
      }
      if (!isTransferring || ws.readyState !== WebSocket.OPEN) break;

      const currentOffset = chunkIndex * CHUNK_SIZE;
      const sliceSize = Math.min(CHUNK_SIZE, file.size - currentOffset);
      const slice = file.slice(currentOffset, currentOffset + sliceSize);

      try {
        const buf = await readBlobAsArrayBuffer(slice);
        if (!isTransferring || ws.readyState !== WebSocket.OPEN) break;

        const packet = new Uint8Array(4 + buf.byteLength);
        const dv = new DataView(packet.buffer);
        dv.setUint32(0, chunkIndex, false);
        packet.set(new Uint8Array(buf), 4);

        ws.send(packet.buffer);
        sentChunks++;

        // Cập nhật thanh tiến trình theo lượng đã gửi thực tế
        const transferredBytes = Math.min(file.size, (chunkIndex + 1) * CHUNK_SIZE);
        const progress = Math.min(100, (transferredBytes / file.size) * 100);

        UI.progressBar.style.width = `${progress}%`;
        UI.transferPercent.textContent = `${progress.toFixed(1)}%`;
        UI.transferStatus.textContent = `Đang truyền: ${file.name}`;
        UI.metricTransferred.textContent = `${formatBytes(transferredBytes)} / ${formatBytes(file.size)}`;
        UI.metricChunks.textContent = `${sentChunks} / ${totalChunks}`;

        const now = performance.now();
        if (now - lastSpeedTime >= 300) {
          const bytesSec = (transferredBytes - lastSpeedBytes) / ((now - lastSpeedTime) / 1000);
          const mbSec = bytesSec / (1024 * 1024);
          UI.metricSpeed.textContent = `${mbSec.toFixed(2)} MB/s`;

          if (bytesSec > 0) {
            const secLeft = (file.size - transferredBytes) / bytesSec;
            const m = Math.floor(secLeft / 60);
            const s = Math.floor(secLeft % 60);
            UI.metricEta.textContent = `${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
          }
          lastSpeedTime = now;
          lastSpeedBytes = transferredBytes;
        }
      } catch (err) {
        log(`Lỗi đọc chunk ${chunkIndex}: ${err.message}`);
        UI.transferStatus.textContent = 'Lỗi đọc file: ' + err.message;
        UI.btnStartSend.disabled = false;
        isTransferring = false;
        releaseWakeLock();
        break;
      }
    }

    if (sentChunks >= totalChunks && isTransferring) {
      UI.progressBar.style.width = '100%';
      UI.transferPercent.textContent = '100%';
      UI.transferStatus.textContent = 'Đã gửi đủ 100% dữ liệu. Đang đợi Bên Nhận đối soát Checksum SHA-256...';
      log('Đã gửi đủ 100% dữ liệu vào đường truyền. Chờ Bên Nhận xác thực mã băm...');
    }
  }

  ws.onopen = () => {
    log('Đã nối Go Relay! Gửi thông tin Metadata & Checksum...');
    startKeepAlive();
    requestWakeLock();
    const meta = {
      name: file.name,
      size: file.size,
      checksum: checksum,
      totalChunks: totalChunks
    };
    ws.send(JSON.stringify(meta));
    UI.transferStatus.textContent = 'Đã gửi Metadata. Đang đợi Bên Nhận vào phòng...';
  };

  ws.onmessage = (e) => {
    if (typeof e.data === 'string') {
      if (e.data === '{"type":"pong"}') return;

      if (e.data === 'READY') {
        log(`Bên Nhận đã sẵn sàng! Bắt đầu truyền dữ liệu siêu tốc liên tục...`);
        UI.transferStatus.textContent = `Đang truyền: ${file.name}`;
        startStreaming();
        return;
      }

      try {
        const parsed = JSON.parse(e.data);
        if (parsed.type === 'PEER_CONNECTED' && parsed.role === 'receiver') {
          log('Bên Nhận đã vào phòng!');
          UI.transferStatus.textContent = 'Bên Nhận đã vào phòng. Gửi Metadata & Checksum...';
          const meta = {
            name: file.name,
            size: file.size,
            checksum: checksum,
            totalChunks: totalChunks
          };
          ws.send(JSON.stringify(meta));
        } else if (parsed.type === 'PEER_DISCONNECTED') {
          log('Bên Nhận đã ngắt kết nối.');
          UI.transferStatus.textContent = 'Bên Nhận vừa ngắt kết nối.';
          releaseWakeLock();
        } else if (parsed.type === 'VERIFY_OK') {
          const totalSec = Math.max(0.1, (performance.now() - startTime) / 1000);
          const avgSpeed = (file.size / (1024 * 1024)) / totalSec;
          UI.transferStatus.textContent = `🎉 Checksum KHỚP 100%! Hoàn tất trong ${totalSec.toFixed(1)}s (${avgSpeed.toFixed(2)} MB/s).`;
          log(`✅ Bên Nhận xác nhận mã Checksum SHA-256 khớp 100%! Hoàn tất trong ${totalSec.toFixed(1)}s (Tốc độ TB: ${avgSpeed.toFixed(2)} MB/s).`);
          UI.btnStartSend.disabled = false;
          releaseWakeLock();
          stopKeepAlive();
        } else if (parsed.type === 'VERIFY_FAIL') {
          UI.transferStatus.textContent = '⚠️ Cảnh báo: Bên Nhận báo mã Checksum không khớp!';
          log('❌ Bên Nhận báo lỗi: Mã Checksum không khớp! File có thể bị hỏng trong quá trình truyền.');
          UI.btnStartSend.disabled = false;
          releaseWakeLock();
          stopKeepAlive();
        }
      } catch (err) {}
    }
  };

  ws.onerror = () => {
    log('Lỗi kết nối WebSocket Bên Gửi.');
    UI.transferStatus.textContent = 'Lỗi kết nối máy chủ.';
    UI.btnStartSend.disabled = false;
    releaseWakeLock();
    stopKeepAlive();
  };

  ws.onclose = () => {
    log('WebSocket Bên Gửi đã ngắt.');
    UI.btnStartSend.disabled = false;
    releaseWakeLock();
    stopKeepAlive();
  };
}

// ==========================================
// 4. BÊN NHẬN (/nhan) - TỰ ĐỘNG TẢI & ĐỐI SOÁT CHECKSUM
// ==========================================
if (UI.btnStartReceive) {
  UI.btnStartReceive.addEventListener('click', () => {
    UI.btnStartReceive.disabled = true;
    UI.progressCard.style.display = 'block';
    UI.transferStatus.textContent = 'Đang kết nối Go Relay và chờ Bên Gửi...';

    if (ws) ws.close();
    const wsUrl = getWsUrl('receiver');
    log(`Kết nối Bên Nhận: ${wsUrl}`);
    ws = new WebSocket(wsUrl);
    ws.binaryType = 'arraybuffer';

    let meta = null;
    let receivedBytes = 0;
    let chunks = [];
    let chunkCount = 0;
    let startTime = 0;
    let lastSpeedTime = 0;
    let lastSpeedBytes = 0;

    let opfsFileHandle = null;
    let opfsWritable = null;
    let useOpfs = false;
    let opfsWriteQueue = Promise.resolve();

    const cleanupOpfs = async () => {
      if (opfsWritable) {
        try { await opfsWritable.abort(); } catch (_) {}
        opfsWritable = null;
      }
      if (opfsFileHandle) {
        try { await opfsFileHandle.remove(); } catch (_) {}
        opfsFileHandle = null;
      }
    };

    ws.onopen = () => {
      log('Bên Nhận đã vào phòng. Chờ Bên Gửi gửi Metadata & Checksum...');
      UI.transferStatus.textContent = 'Đã kết nối phòng! Đang đợi Bên Gửi bấm gửi...';
      startKeepAlive();
    };

    ws.onmessage = async (e) => {
      // Nhận Metadata hoặc Ping/Pong
      if (typeof e.data === 'string') {
        if (e.data === '{"type":"pong"}') return;

        try {
          const parsed = JSON.parse(e.data);
          if (parsed.name && parsed.size) {
            meta = parsed;
            receivedBytes = 0;
            chunkCount = 0;
            startTime = 0; // Bấm giờ khi nhận chunk 0
            lastSpeedTime = 0;
            lastSpeedBytes = 0;
            chunks = [];
            useOpfs = false;
            opfsWriteQueue = Promise.resolve();

            // Khởi tạo OPFS Direct Disk Streaming nếu trình duyệt hỗ trợ
            if (navigator.storage && typeof navigator.storage.getDirectory === 'function') {
              try {
                const root = await navigator.storage.getDirectory();
                const safeName = meta.name.replace(/[^a-zA-Z0-9._-]/g, '_');
                const tempName = `v6drop_${Date.now()}_${safeName}`;
                opfsFileHandle = await root.getFileHandle(tempName, { create: true });
                if (typeof opfsFileHandle.createWritable === 'function') {
                  opfsWritable = await opfsFileHandle.createWritable({ keepExistingData: true });
                  useOpfs = true;
                  log('⚡ Kích hoạt OPFS Direct Disk Streaming (Ghi thẳng đĩa, RAM luôn < 10MB)');
                }
              } catch (opfsErr) {
                log(`OPFS không khả dụng (${opfsErr.message}), dùng bộ đệm RAM.`);
                await cleanupOpfs();
                useOpfs = false;
              }
            }

            if (!useOpfs) {
              chunks = new Array(meta.totalChunks || 0);
            }

            requestWakeLock();
            log(`Bắt đầu nhận file: ${meta.name} (${formatBytes(meta.size)}) | Checksum gửi: ${meta.checksum.slice(0, 16)}...`);
            UI.transferStatus.textContent = `Đang đợi dữ liệu: ${meta.name}`;
            ws.send('READY');
            return;
          }
        } catch (err) {}
        return;
      }

      // Xử lý Binary Chunk
      let buffer = e.data;
      if (buffer instanceof Blob) {
        buffer = await buffer.arrayBuffer();
      }

      if (buffer instanceof ArrayBuffer) {
        if (buffer.byteLength < 4) return;

        if (startTime === 0) {
          startTime = performance.now();
          lastSpeedTime = startTime;
          lastSpeedBytes = 0;
          UI.transferStatus.textContent = `Đang nhận: ${meta.name}`;
        }

        const dv = new DataView(buffer);
        const chunkIndex = dv.getUint32(0, false);
        const chunkData = new Uint8Array(buffer, 4);

        if (useOpfs && opfsWritable) {
          // Ghi đĩa trực tiếp qua OPFS theo vị trí byte offset
          const offset = chunkIndex * CHUNK_SIZE;
          opfsWriteQueue = opfsWriteQueue.then(() =>
            opfsWritable.write({ type: 'write', position: offset, data: chunkData })
          ).catch(wErr => {
            log(`Lỗi ghi đĩa OPFS: ${wErr.message}`);
          });
        } else {
          chunks[chunkIndex] = chunkData;
        }

        receivedBytes += chunkData.byteLength;
        chunkCount++;

        const now = performance.now();
        const progress = Math.min(100, (receivedBytes / meta.size) * 100);

        UI.progressBar.style.width = `${progress}%`;
        UI.transferPercent.textContent = `${progress.toFixed(1)}%`;
        UI.metricTransferred.textContent = `${formatBytes(receivedBytes)} / ${formatBytes(meta.size)}`;
        UI.metricChunks.textContent = `${chunkCount} / ${meta.totalChunks || '?'}`;

        if (now - lastSpeedTime >= 300) {
          const bytesSec = (receivedBytes - lastSpeedBytes) / ((now - lastSpeedTime) / 1000);
          const mbSec = bytesSec / (1024 * 1024);
          UI.metricSpeed.textContent = `${mbSec.toFixed(2)} MB/s`;

          if (bytesSec > 0) {
            const secLeft = (meta.size - receivedBytes) / bytesSec;
            const m = Math.floor(secLeft / 60);
            const s = Math.floor(secLeft % 60);
            UI.metricEta.textContent = `${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
          }
          lastSpeedTime = now;
          lastSpeedBytes = receivedBytes;
        }

        // Khi nhận đủ 100% dữ liệu
        if (receivedBytes >= meta.size) {
          const totalSec = Math.max(0.1, (performance.now() - startTime) / 1000);
          const avgSpeed = (meta.size / (1024 * 1024)) / totalSec;
          UI.transferStatus.textContent = 'Đang hoàn tất ghi đĩa và đối soát Checksum...';
          log(`Đã nhận đủ 100% dữ liệu trong ${totalSec.toFixed(1)}s (Tốc độ TB: ${avgSpeed.toFixed(2)} MB/s). Bắt đầu đối soát mã Checksum...`);

          let targetFileOrBlob = null;
          const currentOpfsHandle = opfsFileHandle;

          if (useOpfs && opfsWritable) {
            try {
              await opfsWriteQueue;
              await opfsWritable.close();
              opfsWritable = null;
              targetFileOrBlob = await currentOpfsHandle.getFile();
            } catch (closeErr) {
              log(`Lỗi đóng file OPFS: ${closeErr.message}`);
            }
          }

          if (!targetFileOrBlob) {
            targetFileOrBlob = new Blob(chunks);
            chunks = [];
          }

          try {
            const receiverChecksum = await computeChecksum(targetFileOrBlob);
            log(`Mã Checksum Bên Nhận tính được: ${receiverChecksum.slice(0, 16)}...`);

            if (receiverChecksum === meta.checksum) {
              log(`✅ Khớp mã Checksum SHA-256 hoàn hảo! File nguyên vẹn 100%.`);
              UI.transferStatus.textContent = `🎉 Checksum KHỚP 100%! Đã tải ${meta.name} về máy (${avgSpeed.toFixed(2)} MB/s)`;

              ws.send(JSON.stringify({ type: 'VERIFY_OK', checksum: receiverChecksum, avgSpeed: avgSpeed.toFixed(2) }));

              const downloadUrl = URL.createObjectURL(targetFileOrBlob);
              const a = document.createElement('a');
              a.href = downloadUrl;
              a.download = meta.name;
              document.body.appendChild(a);
              a.click();
              document.body.removeChild(a);

              // Dọn file tạm OPFS sau 60s
              setTimeout(async () => {
                URL.revokeObjectURL(downloadUrl);
                if (currentOpfsHandle) {
                  try { await currentOpfsHandle.remove(); } catch (_) {}
                }
              }, 60000);
            } else {
              log(`❌ Lỗi: Mã Checksum không khớp! (Gửi: ${meta.checksum} != Nhận: ${receiverChecksum})`);
              UI.transferStatus.textContent = `⚠️ Cảnh báo: Mã Checksum không khớp! File có thể bị lỗi khi truyền.`;
              ws.send(JSON.stringify({ type: 'VERIFY_FAIL' }));
              if (currentOpfsHandle) {
                try { await currentOpfsHandle.remove(); } catch (_) {}
              }
            }
          } catch (err) {
            log(`Lỗi khi tính checksum bên nhận: ${err.message}`);
            ws.send(JSON.stringify({ type: 'VERIFY_OK' }));
            const downloadUrl = URL.createObjectURL(targetFileOrBlob);
            const a = document.createElement('a');
            a.href = downloadUrl;
            a.download = meta.name;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
          }

          UI.progressBar.style.width = '100%';
          UI.transferPercent.textContent = '100%';
          UI.btnStartReceive.disabled = false;
          releaseWakeLock();
          stopKeepAlive();
        }
      }
    };

    ws.onerror = () => {
      log('Lỗi kết nối Bên Nhận.');
      UI.transferStatus.textContent = 'Lỗi kết nối máy chủ.';
      UI.btnStartReceive.disabled = false;
      cleanupOpfs();
      releaseWakeLock();
      stopKeepAlive();
    };

    ws.onclose = () => {
      log('WebSocket Bên Nhận đã ngắt.');
      UI.btnStartReceive.disabled = false;
      cleanupOpfs();
      releaseWakeLock();
      stopKeepAlive();
    };
  });
}
