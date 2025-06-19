# Docker Build and Run Process Explained

## What is Docker?

Docker is a containerization platform that packages applications and their dependencies into lightweight, portable containers. Think of it like a shipping container for software - everything the app needs is bundled together.

## Key Concepts

### 1. **Docker Image**
- A read-only template containing the application and all its dependencies
- Like a "snapshot" or "blueprint" of a complete environment
- Built from instructions in a `Dockerfile`

### 2. **Docker Container**
- A running instance of a Docker image
- Like starting a virtual machine, but much lighter weight
- Isolated from the host system

## Our Dockerfile Step-by-Step

```dockerfile
FROM agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim
```
**What it does:** 
- Starts with a pre-built base image from Docker Hub
- This image already contains:
  - Python 3.10.13
  - A trained ML model (`model.bin`)
  - Basic Python environment

**Why:** Instead of installing Python and the model ourselves, we use a ready-made foundation

---

```dockerfile
RUN pip install pandas scikit-learn==1.5.0 pyarrow
```
**What it does:**
- Installs required Python packages inside the container
- `RUN` executes commands during the build process
- Downloads and installs packages into the image

**Why:** Our script needs these specific libraries to work

---

```dockerfile
COPY batch_inference.py /app/
```
**What it does:**
- Copies our Python script from the local machine into the container
- Source: `batch_inference.py` (on your computer)
- Destination: `/app/batch_inference.py` (inside container)

**Why:** The container needs our code to run

---

```dockerfile
WORKDIR /app
```
**What it does:**
- Sets the working directory inside the container to `/app`
- Like doing `cd /app` in a terminal

**Why:** Our script expects to find `model.bin` in the current directory

---

```dockerfile
CMD python batch_inference.py 2023 5
```
**What it does:**
- Defines the default command to run when the container starts
- Runs our script with arguments: year=2023, month=5 (May 2023)

**Why:** This is what we want the container to do when it starts

## Build vs Run Process

### `docker build -t batch-inference .`

**"Building" means:**
1. 📥 **Download base image** (if not cached)
   - Pulls `agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim`
   - Downloads Python environment + pre-trained model

2. 🔧 **Execute build steps** (each Dockerfile instruction)
   - Install pandas, scikit-learn, pyarrow
   - Copy our script into the image
   - Set working directory

3. 💾 **Create new image**
   - Saves the result as `batch-inference:latest`
   - This image is now stored on your machine

**Result:** A complete, self-contained image ready to run

### `docker run batch-inference`

**"Running" means:**
1. 🚀 **Create container** from the image
   - Starts a new isolated environment
   - Like booting up a lightweight VM

2. ⚡ **Execute CMD**
   - Runs: `python batch_inference.py 2023 5`
   - Downloads May 2023 taxi data
   - Loads the pre-trained model
   - Makes predictions
   - Outputs the mean predicted duration

3. 🛑 **Container stops** when script finishes
   - Container exits after printing results

## What Happens When Our Script Runs

```python
# 1. Load the pre-trained model (already in container)
with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)

# 2. Download May 2023 data from the internet
data_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-05.parquet'
urllib.request.urlretrieve(data_url, 'yellow_tripdata_2023-05.parquet')

# 3. Process the data (filter, clean, transform)
df = read_data('yellow_tripdata_2023-05.parquet')

# 4. Make predictions using the model
y_pred = model.predict(X_val)

# 5. Calculate and print the mean predicted duration
mean_duration = np.mean(y_pred)
print(f"Mean predicted duration: {mean_duration:.2f}")
```

## Timeline of Operations

| Step | Operation | Time | What's Happening |
|------|-----------|------|------------------|
| 1 | `docker build` | ~2-3 min | Downloads base image, installs packages |
| 2 | `docker run` starts | <1 sec | Container starts |
| 3 | Download data | 30-60 sec | Downloads ~52MB parquet file |
| 4 | Load & process | 10-30 sec | Reads data, applies transformations |
| 5 | Predict | 5-10 sec | ML model makes predictions |
| 6 | Print result | <1 sec | Shows mean predicted duration |
| 7 | Container exits | <1 sec | Script finished, container stops |

## Why Use Docker for ML?

1. **Reproducibility**: Same environment everywhere
2. **Dependency Management**: All packages bundled together
3. **Isolation**: Won't conflict with your local Python setup
4. **Portability**: Runs identically on any machine with Docker
5. **Production Ready**: Easy to deploy to cloud services

## Common Docker Commands

```bash
# Build an image from Dockerfile
docker build -t my-app .

# Run a container
docker run my-app

# List running containers
docker ps

# List all images
docker images

# Remove a container
docker rm <container-id>

# Remove an image
docker rmi <image-name>
```

## Our Specific Use Case

We're using Docker to:
1. **Package** our batch inference script with exact dependencies
2. **Use** a pre-trained model without managing Python environments
3. **Ensure** the same scikit-learn version (1.5.0) as the model was trained with
4. **Run** the inference in a clean, isolated environment
5. **Get** reproducible results for the homework submission

The container downloads real taxi data, runs real ML inference, and gives us the actual answer for Q6!