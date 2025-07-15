console.log('Enhanced Pipeline JavaScript loaded successfully');

let processingActive = false;
let frameCount = 0;
let fpsStartTime = Date.now();
let webcamStream = null;
let totalFramesProcessed = 0;
let totalInferenceTime = 0;
let recentClassifications = [];

// FPS Control
let targetFPS = 10; // Default 10 FPS
let frameInterval = 1000 / targetFPS; // 100ms for 10 FPS

function updateTargetFPS(newFPS) {
    targetFPS = Math.max(10, newFPS); // Minimum 10 FPS as requested
    frameInterval = 1000 / targetFPS;
    document.getElementById('targetFPS').textContent = `Target: ${targetFPS} FPS`;
    
    // Update button states
    const decreaseBtn = document.getElementById('fpsDecrease');
    if (targetFPS <= 10) {
        decreaseBtn.disabled = true;
        decreaseBtn.classList.add('opacity-50', 'cursor-not-allowed');
    } else {
        decreaseBtn.disabled = false;
        decreaseBtn.classList.remove('opacity-50', 'cursor-not-allowed');
    }
    
    console.log(`FPS updated to: ${targetFPS}, interval: ${frameInterval}ms`);
}

// FPS Control Event Listeners
document.getElementById('fpsIncrease').addEventListener('click', function () {
    updateTargetFPS(targetFPS + 5);
});

document.getElementById('fpsDecrease').addEventListener('click', function () {
    updateTargetFPS(targetFPS - 5);
});

// Initialize button states
updateTargetFPS(targetFPS);

document.getElementById('webcamButton').addEventListener('click', async function () {
    console.log('Webcam button clicked');
    try {
        console.log('Requesting webcam access...');
        webcamStream = await navigator.mediaDevices.getUserMedia({ video: true });
        console.log('Webcam access granted');
        const webcamVideo = document.getElementById('webcamVideo');
        webcamVideo.srcObject = webcamStream;
        webcamVideo.style.display = 'block';
        
        document.getElementById('webcamButton').style.display = 'none';
        document.getElementById('stopButton').style.display = 'block';
        processingActive = true;
        
        // Reset counters
        resetCounters();
        updateCurrentClassification('Starting webcam...', '', '');
        
        // Start processing webcam frames
        processWebcamFrames();
    } catch (error) {
        console.error('Error accessing webcam:', error);
        alert('Could not access webcam. Please check permissions and ensure you are using HTTPS or localhost.');
    }
});

document.getElementById('stopButton').addEventListener('click', function () {
    processingActive = false;
    document.getElementById('stopButton').style.display = 'none';
    document.getElementById('webcamButton').style.display = 'block';
    
    if (webcamStream) {
        webcamStream.getTracks().forEach(track => track.stop());
        webcamStream = null;
        document.getElementById('webcamVideo').style.display = 'none';
    }
    
    updateCurrentClassification('Stopped', '', '');
});

function resetCounters() {
    frameCount = 0;
    fpsStartTime = Date.now();
    totalFramesProcessed = 0;
    totalInferenceTime = 0;
    recentClassifications = [];
    
    document.getElementById('frameCounter').textContent = 'Frames this second: 0';
    document.getElementById('fpsCounter').textContent = 'FPS: 0';
    document.getElementById('totalFrames').textContent = '0';
    document.getElementById('avgFPS').textContent = '0.0';
    document.getElementById('avgInference').textContent = '0.0ms';
    document.getElementById('recentClassifications').innerHTML = '';
}

function processWebcamFrames() {
    if (!processingActive) return;
    
    const video = document.getElementById('webcamVideo');
    const canvas = document.getElementById('frameCanvas');
    const ctx = canvas.getContext('2d');
    
    if (video.videoWidth > 0) {
        canvas.width = video.videoWidth;
        canvas.height = video.videoHeight;
        
        // Draw current frame to canvas
        ctx.drawImage(video, 0, 0);
        
        // Convert canvas to blob and send
        canvas.toBlob(async (blob) => {
            if (blob && processingActive) {
                const formData = new FormData();
                formData.append('frame', blob, `frame_${Date.now()}.jpg`);
                
                try {
                    await fetch('/api/process-frame/', {
                        method: 'POST',
                        body: formData
                    });
                    updateCounters();
                } catch (error) {
                    console.error('Error sending frame:', error);
                }
            }
        }, 'image/jpeg', 0.8);
    }
    
    // Process next frame using dynamic FPS interval
    setTimeout(() => processWebcamFrames(), frameInterval);
}

function updateCounters() {
    frameCount++;
    totalFramesProcessed++;
    
    // Update display
    document.getElementById('frameCounter').textContent = `Frames this second: ${frameCount}`;
    document.getElementById('totalFrames').textContent = totalFramesProcessed;
    
    const now = Date.now();
    const elapsed = (now - fpsStartTime) / 1000;
    if (elapsed >= 1) {
        const fps = frameCount / elapsed;
        document.getElementById('fpsCounter').textContent = `FPS: ${fps.toFixed(1)} / ${targetFPS}`;
        document.getElementById('avgFPS').textContent = fps.toFixed(1);
        frameCount = 0; // Reset for next second
        fpsStartTime = now;
    }
}

function updateCurrentClassification(className, confidence, inferenceTime) {
    document.getElementById('currentClassification').textContent = className || 'Unknown';
    document.getElementById('confidence').textContent = confidence ? `Confidence: ${confidence}` : 'Confidence: --';
    document.getElementById('inferenceTime').textContent = inferenceTime ? `Inference Time: ${inferenceTime}ms` : 'Inference Time: --';
}

function addRecentClassification(data) {
    const container = document.getElementById('recentClassifications');
    
    // Create new classification entry
    const entry = document.createElement('div');
    entry.className = 'p-3 bg-gray-50 rounded flex justify-between items-center';
    
    const timestamp = new Date(data.timestamp * 1000).toLocaleTimeString();
    entry.innerHTML = `
        <div>
            <span class="font-semibold text-gray-800">${data.class_name}</span>
            <span class="text-sm text-gray-500 ml-2">(${data.frame_type})</span>
        </div>
        <div class="text-right">
            <div class="text-sm text-gray-600">${timestamp}</div>
            <div class="text-xs text-gray-500">${data.inference_time.toFixed(3)}s</div>
        </div>
    `;
    
    // Add to top of list
    container.insertBefore(entry, container.firstChild);
    
    // Keep only last 10 entries
    while (container.children.length > 10) {
        container.removeChild(container.lastChild);
    }
    
    // Update recent classifications array
    recentClassifications.unshift(data);
    if (recentClassifications.length > 10) {
        recentClassifications.pop();
    }
}

function updatePerformanceStats(data) {
    totalFramesProcessed++;
    totalInferenceTime += data.inference_time;
    
    // Update total frames
    document.getElementById('totalFrames').textContent = totalFramesProcessed;
    
    // Update average inference time
    const avgInference = (totalInferenceTime / totalFramesProcessed) * 1000; // Convert to ms
    document.getElementById('avgInference').textContent = `${avgInference.toFixed(1)}ms`;
    
    // Calculate FPS over longer period
    if (recentClassifications.length > 1) {
        const timeSpan = recentClassifications[0].timestamp - recentClassifications[recentClassifications.length - 1].timestamp;
        if (timeSpan > 0) {
            const avgFPS = (recentClassifications.length - 1) / timeSpan;
            document.getElementById('avgFPS').textContent = avgFPS.toFixed(1);
        }
    }
}

async function connectWebSocket() {
    // Use current host and port for WebSocket connection
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const host = window.location.host;
    const wsUrl = `${protocol}//${host}/ws`;
    console.log('Connecting to WebSocket:', wsUrl);
    
    let socket = new WebSocket(wsUrl);
    
    socket.onmessage = function (event) {
        try {
            // Try to parse as JSON first
            const data = JSON.parse(event.data);
            
            if (data.class_name) {
                // Update current classification
                updateCurrentClassification(
                    data.class_name.replace(/_/g, ' '),
                    data.confidence || '',
                    data.inference_time ? (data.inference_time * 1000).toFixed(1) : ''
                );
                
                // Add to recent classifications
                addRecentClassification(data);
                
                // Update performance stats
                updatePerformanceStats(data);
            }
        } catch (e) {
            // Handle simple string responses (legacy)
            console.log("Received simple message:", event.data);
            
            // Extract class name from filename if it follows the pattern
            const filename = event.data;
            if (filename.includes('classified_')) {
                const parts = filename.split('_');
                if (parts.length >= 2) {
                    const className = parts[1].replace(/\./g, '');
                    updateCurrentClassification(className.replace(/_/g, ' '), '', '');
                }
            }
        }
    };
    
    socket.onerror = function(error) {
        console.error('WebSocket error:', error);
        updateCurrentClassification('Connection Error', '', '');
    };
    
    socket.onclose = function() {
        console.log('WebSocket closed, attempting to reconnect...');
        updateCurrentClassification('Reconnecting...', '', '');
        setTimeout(connectWebSocket, 3000);
    };
    
    socket.onopen = function() {
        console.log('WebSocket connected');
        updateCurrentClassification('Connected - Ready for input', '', '');
    };
}

connectWebSocket();