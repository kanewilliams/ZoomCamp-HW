# MLOps ZoomCamp 2025 - Module04 Homework Results
## Deployment (Batch Inference)

**Completed by Kane Williams | June 2025**

This module focuses on deploying ML models for batch inference using Docker containers and explores the differences between online and offline deployment strategies.

---

## Homework Answers

### Q1. Notebook Execution (March 2023 Data)
**Question:** What's the standard deviation of the predicted duration for March 2023 dataset?

**Answer:** `6.25`

**Process:**
- Modified starter notebook to use local March 2023 taxi data
- Loaded pre-trained model from `model.bin`
- Calculated standard deviation using `np.std(y_pred)`

### Q2. Preparing the Output
**Question:** What's the size of the output parquet file?

**Answer:** `65M`

**Process:**
- Created artificial `ride_id` column: `f'{year:04d}/{month:02d}_' + df.index.astype('str')`
- Saved results with `engine='pyarrow'`, `compression=None`
- File contained 3,316,216 records with ride_id and predicted_duration columns

### Q3. Creating the Scoring Script
**Question:** Which command do you need to execute to convert notebook to script?

**Answer:** `jupyter nbconvert --to script starter.ipynb`

**Note:** This is the standard method for converting Jupyter notebooks to Python scripts, preserving cell structure as comments.

### Q4. Virtual Environment (pipenv)
**Question:** What's the first hash for the Scikit-Learn dependency in Pipfile.lock?

**Answer:** `sha256:057b991ac64b3e75c9c04b5f9395eaf19a6179244c089afdebaad98264bff37c`

**Process:**
- Installed pipenv and created environment with `scikit-learn==1.5.0`
- Generated `Pipfile.lock` with dependency hashes for reproducible builds
- Retrieved first hash from scikit-learn section

### Q5. Parametrize the Script
**Question:** What's the mean predicted duration for April 2023?

**Answer:** `14.29`

**Process:**
- Modified script to accept year/month CLI arguments
- Downloaded April 2023 data and ran: `python starter.py 2023 4`
- Added mean calculation and print statement

### Q6. Docker Container
**Question:** What's the mean predicted duration for May 2023 when run in Docker?

**Answer:** `0.19`

**Process:**
- Built Docker image using base: `agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim`
- Created containerized batch inference script
- Container downloaded May 2023 data (3,399,555 records) and executed inference
- Used pre-trained model already included in base image

---

## Key Technical Insights

### Scikit-Learn Version Compatibility
The dramatic difference between local results (Q5: 14.29) and Docker results (Q6: 0.19) highlighted a critical lesson:

- **Local environment**: scikit-learn 1.6.1 (from uv)
- **Docker environment**: scikit-learn 1.5.0 (matching model training)

This version mismatch caused `InconsistentVersionWarning` and unreliable predictions locally. **Always match the scikit-learn version between training and inference environments.**

### Docker Architecture

**Base Image Contents:**
```
FROM agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim
# Contains:
# - Python 3.10.13
# - Pre-trained model (model.bin)
# - Minimal Python environment
```

**Our Additions:**
```dockerfile
RUN pip install pandas scikit-learn==1.5.0 pyarrow
COPY batch_inference.py /app/
WORKDIR /app
CMD python batch_inference.py 2023 5
```

### Batch Inference Pipeline
1. **Data Download**: Downloads parquet files from public NYC taxi dataset
2. **Preprocessing**: Applies duration filtering and categorical encoding
3. **Prediction**: Uses DictVectorizer + LinearRegression model
4. **Output**: Saves predictions with ride IDs to parquet format

---

## Tools and Technologies Used

- **uv**: Modern Python dependency management
- **pipenv**: Virtual environment with lock files for reproducibility
- **Docker**: Containerization for deployment consistency
- **scikit-learn**: ML model (DictVectorizer + LinearRegression)
- **pandas**: Data processing and manipulation
- **pyarrow**: Efficient parquet file handling
- **Jupyter/nbconvert**: Notebook-to-script conversion

---

## Repository Structure
```
Module04/
├── homework_files/
│   ├── homework.md              # Original homework instructions
│   ├── model.bin               # Pre-trained ML model
│   ├── starter.ipynb           # Original Jupyter notebook
│   ├── starter.py              # Converted Python script
│   ├── batch_inference.py      # Docker-optimized script
│   ├── Dockerfile              # Container definition
│   ├── Pipfile                 # pipenv dependencies
│   ├── Pipfile.lock           # Locked dependency versions
│   └── Docker_Explanation.md   # Docker concepts guide
├── output.parquet              # Q2 results (March 2023)
├── output_2023-04.parquet      # Q5 results (April 2023)
└── Module04.md                 # This file
```

---

## Performance Metrics

| Dataset | Records | Mean Duration | Std Dev | File Size |
|---------|---------|---------------|---------|-----------|
| March 2023 | 3,316,216 | ~12.8 | 6.25 | 65M |
| April 2023 | ~3.2M | 14.29 | - | - |
| May 2023 (Docker) | 3,399,555 | 0.19 | - | - |

---

## Lessons Learned

1. **Version Compatibility**: ML model serialization requires exact scikit-learn versions
2. **Docker Benefits**: Ensures reproducible environments across different machines
3. **Batch Processing**: Efficient for processing large datasets periodically
4. **Data Persistence**: Docker containers are ephemeral - use volumes for persistent storage
5. **Model Validation**: Always validate model outputs when changing environments

---

**Homework Submission Date:** June 20, 2025  
**Docker Image Size:** 1.08GB  
**Total Processing Time:** ~3 minutes (including 52MB data download)