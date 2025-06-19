FROM agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim

# Install required dependencies
RUN pip install pandas scikit-learn==1.5.0 pyarrow

# Copy our script
COPY batch_inference.py /app/

# The model.bin is already in the image at /app/model.bin
WORKDIR /app

# Download May 2023 data and run inference
CMD python batch_inference.py 2023 5