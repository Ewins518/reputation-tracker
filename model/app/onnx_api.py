import uvicorn
import onnxruntime
import numpy as np
from fastapi import FastAPI, HTTPException
from transformers import DistilBertTokenizer

app = FastAPI()

onnx_file_path = "distilbert_model.onnx"
ort_session = onnxruntime.InferenceSession(onnx_file_path)
tokenizer = DistilBertTokenizer.from_pretrained('distilbert-base-uncased')

def preprocess_text(text, max_sequence_length=512): 
    tokens = tokenizer.encode(text, max_length=max_sequence_length, truncation=True)
    padding_length = max_sequence_length - len(tokens)
    tokens += [0] * padding_length  # Padding token ID for BERT
    return np.array(tokens).reshape(1, -1)

def predict_sentiment(text):
    input_ids = preprocess_text(text)

    ort_inputs = {'input_ids': input_ids}
    ort_outs = ort_session.run(None, ort_inputs)

    logits = ort_outs[0]
    predicted_class = np.argmax(logits, axis=1).item()

    return predicted_class

@app.get("/")
def read_root():
    return {"Hello": "Welcome to the .onnx test api"}

@app.post("/predict_sentiment/{text}")
def predict_sentiment_api(text: str):
    try:
        sentiment = predict_sentiment(text)
        #return {"sentiment": "positive" if sentiment == 1 else "negative"}
        return sentiment
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
