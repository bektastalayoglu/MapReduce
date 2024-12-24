# MapReduce

This project contains multiple tasks that use Python scripts to process different datasets. Each task is located in its own directory (e.g., `Task1`, `Task2`, etc.). Follow the instructions below to set up the environment and execute the tasks.

---

## Setup Instructions

### 1. Make `setup.sh` Executable
Before setting up the environment, make the `setup.sh` file executable by running:
```bash
chmod +x setup.sh
```

### 2. Run the Setup Script
Execute the `setup.sh` script to create the virtual environment and install the required dependencies:
```bash
./setup.sh
```

### 3. Activate the Virtual Environment
Activate the virtual environment to ensure all required Python libraries are available:
```bash
source venv/bin/activate
```

---

## Task Instructions

Each task has a Python script and a dataset file. Follow these steps to navigate to the correct directory and run each task.

### Task 1
- **Navigate to Task 1 directory**:
  ```bash
  cd Task1
  ```
- **Run the script** using the dataset `movies.csv`:
  ```bash
  python task1.py movies.csv > output.txt
  ```
- **Output** will be saved in `output.txt`.

---

### Task 2
- **Navigate to Task 2 directory**:
  ```bash
  cd ../Task2
  ```
- **Run the script** using the dataset `web-Google.txt`:
  ```bash
  python task2.py web-Google.txt > output.txt
  ```
- **Output** will be saved in `output.txt`.

---

### Task 3
- **Navigate to Task 3 directory**:
  ```bash
  cd ../Task3
  ```
- **Run the script** using the dataset `Iris.csv`:
  ```bash
  python task3.py Iris.csv > output.txt
  ```
- **Output** will be saved in `output.txt`.

---

### Task 4
- **Navigate to Task 4 directory**:
  ```bash
  cd ../Task4
  ```
- **Run the script** using the dataset `A.txt`:
  ```bash
  python task4.py A.txt > output.txt
  ```
- **Output** will be saved in `output.txt`.

---

## Notes
1. **Activate the virtual environment**: Always activate the environment (`source venv/bin/activate`) before running any task.
2. **Reinstall dependencies**:
   If you encounter issues, reinstall dependencies by running:
   ```bash
   pip install -r requirements.txt
   ```
3. **Output Files**: All outputs will be saved in the respective task directories as `output.txt`.
4. **Virtual Environment**: The virtual environment will handle all dependencies required for this project.
5. **Deactivate the virtual environment**: Run ```bash deactivate ``` to exit the environment.
---

## Directory Structure
The project directory is structured as follows:
```
Project/
├── setup.sh
├── requirements.txt
├── venv/                # Virtual environment (created after setup)
├── Task1/
│   ├── task1.py
│   └── movies.csv
├── Task2/
│   ├── task2.py
│   └── web-Google.txt
├── Task3/
│   ├── task3.py
│   └── Iris.csv
└── Task4/
    ├── task4.py
    └── A.txt
```

---





