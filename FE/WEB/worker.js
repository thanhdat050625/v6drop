self.onmessage = async (e) => {
  const { file, chunkSize } = e.data;
  
  try {
    // Với file <= 300MB, tính toàn vẹn SHA-256 toàn bộ file
    if (file.size <= 300 * 1024 * 1024) {
      const buffer = await file.arrayBuffer();
      const hashBuffer = await crypto.subtle.digest("SHA-256", buffer);
      const hashArray = Array.from(new Uint8Array(hashBuffer));
      const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
      self.postMessage({ type: "HASH_COMPLETE", hash: hashHex });
      return;
    }

    // Với file video siêu lớn (> 300MB), băm mẫu 64MB đầu + 64MB cuối để tránh tràn RAM trình duyệt
    const sampleSize = 64 * 1024 * 1024;
    const startSlice = await file.slice(0, sampleSize).arrayBuffer();
    const endSlice = await file.slice(file.size - sampleSize, file.size).arrayBuffer();
    
    const combined = new Uint8Array(startSlice.byteLength + endSlice.byteLength);
    combined.set(new Uint8Array(startSlice), 0);
    combined.set(new Uint8Array(endSlice), startSlice.byteLength);
    
    const hashBuffer = await crypto.subtle.digest("SHA-256", combined.buffer);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join('') + `_sz${file.size}`;

    self.postMessage({ type: "HASH_COMPLETE", hash: hashHex });
  } catch (err) {
    // Fallback: Nhận diện theo metadata
    const fallbackId = `fb_${file.name}_${file.size}_${file.lastModified}`;
    self.postMessage({ type: "HASH_COMPLETE", hash: fallbackId });
  }
};
