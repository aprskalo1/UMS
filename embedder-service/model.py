from transformers import AutoProcessor, AutoModel
import torch
import numpy as np
from config import TARGET_SAMPLING_RATE
from logger import logger
import time

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

processor = AutoProcessor.from_pretrained(
    "m-a-p/MERT-v1-95M",
    trust_remote_code=True,
    use_fast=False
)
model = AutoModel.from_pretrained(
    "m-a-p/MERT-v1-95M",
    trust_remote_code=True
).to(device)
model.eval()


def get_embedding(waveform: torch.Tensor) -> np.ndarray:
    start = time.time()
    try:
        audio = waveform.squeeze(0).cpu().numpy()

        inputs = processor(
            raw_speech=audio,
            sampling_rate=TARGET_SAMPLING_RATE,
            return_tensors="pt"
        )
        inputs = {k: v.to(device) for k, v in inputs.items()}

        with torch.no_grad():
            hidden = model(**inputs).last_hidden_state
            embedding = hidden.mean(dim=1).squeeze()

        vec = embedding.cpu().numpy().astype("float32")
        duration = time.time() - start
        logger.info(f"get_embedding took {duration:.3f}s")
        return vec

    except Exception as e:
        logger.exception(f"Error in get_embedding: {e}")
        raise
