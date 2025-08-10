# PySpark Assignment

This repository contains a collection of PySpark exercises, utility functions, and test cases for practicing big data processing using the PySpark DataFrame API.  
The tasks are structured by questions and cover various Spark concepts including file handling, transformations, filtering, date functions, and optimization.

## 📂 Project Structure

## 📜 Questions & Tasks

### **Q1: Data Filtering & Aggregations**
- Filter records using multiple conditions.
- Group data by keys and perform aggregations like count, sum, avg.
- Handle NULL values efficiently.

### **Q2: Data Masking**
- Mask sensitive information like credit card numbers.
- Generate a new column `masked_card_number` with masked values.

### **Q3: Partitioning**
- Check current partition count of DataFrames.
- Repartition or coalesce DataFrames for performance optimization.
- Demonstrate effect of partitioning on shuffles.

### **Q4: File Reading**
- Read CSV and JSON files using different Spark read methods.
- Apply custom schemas for efficient parsing.
- Print number of partitions for loaded DataFrames.

### **Q5: JSON Flattening & Schema Handling**
- Dynamically read files with `dynamic_file()` function.
- Flatten nested JSON using `flatten_json()`.
- Convert camelCase column names to snake_case.
- Add current date and extract year/month.
  
---

## ⚡ Features

- **Dynamic File Reader** — Supports CSV, JSON, and more.
- **Reusable Utilities** — Common helper functions across questions.
- **Unit Testing** — Automated test cases using Python's `unittest` framework.
- **Optimization** — Partitioning and broadcast joins for large datasets.

---

## 🛠️ Setup & Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/GowdhamanBJ-7/pyspark_assignment.git
   cd pyspark_assignment
