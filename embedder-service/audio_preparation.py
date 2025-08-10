import os
import torch
import torchaudio
import noisereduce as nr
import numpy as np
from transformers import AutoProcessor, AutoModel
from config import TARGET_SAMPLING_RATE
from logger import logger
import time

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
processor = AutoProcessor.from_pretrained(
    "m-a-p/MERT-v1-95M", trust_remote_code=True, use_fast=False
)
model = AutoModel.from_pretrained(
    "m-a-p/MERT-v1-95M", trust_remote_code=True
).to(device)
model.eval()


def trim_silence_torch(waveform: torch.Tensor, top_db: float = 60.0) -> torch.Tensor:
    y = waveform.squeeze(0)  # shape: [T]
    abs_y = y.abs()
    max_amp = float(abs_y.max())
    if max_amp == 0:
        return waveform

    threshold = max_amp * (10 ** (-top_db / 20.0))
    non_silence = torch.where(abs_y > threshold)[0]
    if non_silence.numel() == 0:
        return waveform[:, :1]

    start = non_silence[0].item()
    end = non_silence[-1].item()
    return waveform[:, start: end + 1]


def normalize_loudness(waveform: torch.Tensor, target_d_bfs: float = -23.0) -> torch.Tensor:
    rms = waveform.pow(2).mean().sqrt()
    current_dBFS = 20 * torch.log10(rms + 1e-9)
    gain = target_d_bfs - current_dBFS
    return waveform * (10 ** (gain / 20))


def denoise(waveform: torch.Tensor, sr: int) -> torch.Tensor:
    audio = waveform.squeeze(0).cpu().numpy()
    reduced = nr.reduce_noise(y=audio, sr=sr)
    return torch.from_numpy(reduced).unsqueeze(0).to(waveform.dtype)


def load_and_prep(pth: str, do_denoise: bool = False) -> torch.Tensor:
    start = time.time()
    waveform, sr = torchaudio.load(pth)

    # Resample
    if sr != TARGET_SAMPLING_RATE:
        waveform = torchaudio.transforms.Resample(sr, TARGET_SAMPLING_RATE)(waveform)
        sr = TARGET_SAMPLING_RATE

    # Mono mix
    if waveform.size(0) > 1:
        waveform = waveform.mean(dim=0, keepdim=True)

    # Trim silence
    waveform = trim_silence_torch(waveform, top_db=60.0)

    # Normalize loudness
    waveform = normalize_loudness(waveform)

    # Denoise (optional) -> this can be computationally expensive, not needed if the audio is already clean
    if do_denoise:
        waveform = denoise(waveform, sr)

    duration = time.time() - start
    logger.info(f"load_and_prep({os.path.basename(pth)}) took {duration:.3f}s")
    return waveform


def get_embedding_from_waveform(waveform: torch.Tensor) -> np.ndarray:
    with torch.no_grad():
        audio_np = waveform.squeeze(0).cpu().numpy()
        inputs = processor(raw_speech=audio_np,
                           sampling_rate=TARGET_SAMPLING_RATE,
                           return_tensors="pt")
        inputs = {k: v.to(device) for k, v in inputs.items()}
        hidden = model(**inputs).last_hidden_state
        emb = hidden.mean(dim=1).squeeze()
    return emb.cpu().numpy().astype("float32")


def windowed_embedding(
        waveform: torch.Tensor,
        sr: int = TARGET_SAMPLING_RATE,
        window_s: float = 5.0,
        stride_s: float = 2.5
) -> np.ndarray:
    win_len = int(window_s * sr)
    step = int(stride_s * sr)
    vecs = []
    total_samples = waveform.size(1)

    for start in range(0, max(1, total_samples - win_len + 1), step):
        w = waveform[:, start:start + win_len]
        vecs.append(get_embedding_from_waveform(w))

    if not vecs:
        return get_embedding_from_waveform(waveform)

    return np.mean(vecs, axis=0)


if __name__ == "__main__":
    import sys

    path = sys.argv[1] if len(sys.argv) > 1 else None
    if not path or not os.path.isfile(path):
        print("Usage: python audio_preparation.py /path/to/file.wav")
        sys.exit(1)

    wav = load_and_prep(path, do_denoise=False)
    emb_full = get_embedding_from_waveform(wav)
    emb_win = windowed_embedding(wav)

    print(f"Full-track embedding shape: {emb_full.shape}")
    print(f"Window-averaged embedding shape: {emb_win.shape}")
