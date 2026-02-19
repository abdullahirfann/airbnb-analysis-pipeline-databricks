# 🚀 Databricks End-to-End Data Engineering Pipeline

An end-to-end modern Data Engineering project built on Databricks implementing a scalable **Medallion Architecture (Bronze → Silver → Gold)** with Delta Lake, automated transformations, and KPI-driven analytics.

---

# 🏗️ Architecture Overview

Bronze → Silver → Gold → BI Layer

---

# 📊 Dashboard & Visualization Layer

Below are the business intelligence dashboards built on top of the **Gold Layer KPI tables**.

---

## 🖥️ 1. Host Dashboard

**Purpose:**  
Monitor host-level performance, revenue generation, booking trends, and engagement metrics.

### Key KPIs:
- Total Listings
- Active Hosts
- Revenue per Host
- Booking Trends
- Host Rating Distribution

### Visualization Includes:
- Line Chart (Revenue Trend)
- Bar Chart (Top Performing Hosts)
- KPI Cards
- Rating Distribution Histogram

### 📷 Dashboard Preview:
![Host Dashboard](images/host_dashboard.png)

---

## 🌍 2. Geographical Dashboard

**Purpose:**  
Analyze performance across regions, cities, and countries.

### Key KPIs:
- Revenue by Location
- Bookings by Country
- Average Price by Region
- Occupancy Rate by Geography

### Visualization Includes:
- Map Visualization
- Choropleth Map
- Revenue by City Bar Chart
- Regional Trend Line Chart

### 📷 Dashboard Preview:
![Geographical Dashboard](images/geographical_dashboard.png)

---

## 📈 3. Performance Intelligence Dashboard

**Purpose:**  
Operational and business performance monitoring.

### Key KPIs:
- Conversion Rate
- Average Booking Duration
- Revenue Growth Rate
- Cancellation Rate
- Occupancy Trends

### Visualization Includes:
- KPI Summary Cards
- Funnel Chart
- Time-Series Trend Analysis
- Cancellation Rate Breakdown

### 📷 Dashboard Preview:
![Performance Intelligence Dashboard](images/performance_dashboard.png)

---

## 🤖 4. AI Intel Dashboard

**Purpose:**  
Insights derived from ML/AI models such as pricing optimization, anomaly detection, and forecasting.

### Key KPIs:
- Predicted Revenue
- Price Optimization Impact
- Demand Forecast
- Anomaly Detection Alerts
- Model Accuracy Metrics

### Visualization Includes:
- Forecast Line Chart (Actual vs Predicted)
- Feature Importance Chart
- Anomaly Detection Timeline
- Model Performance Metrics

### 📷 Dashboard Preview:
![AI Intel Dashboard](images/ai_dashboard.png)

---

## 💼 5. Business Dashboard

**Purpose:**  
Executive-level business overview and financial performance summary.

### Key KPIs:
- Total Revenue
- Monthly Recurring Revenue (MRR)
- Customer Lifetime Value (CLV)
- Total Bookings
- Revenue Growth %

### Visualization Includes:
- Executive KPI Cards
- Revenue Growth Line Chart
- Bookings Trend Chart
- Revenue Breakdown Pie Chart

### 📷 Dashboard Preview:
![Business Dashboard](images/business_dashboard.png)

---

# ⚡ Optimization Techniques

- Delta Lake Optimize & Vacuum
- Z-Ordering
- Partitioning Strategy
- Incremental MERGE Operations
- Cluster Performance Tuning
