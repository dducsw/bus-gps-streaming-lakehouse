import os
import pandas as pd
import matplotlib.pyplot as plt

# File paths
CSV_PATH = "scripts/benchmark_results.csv"
OUTPUT_IMAGE = "scripts/benchmark_chart.png"

def plot_benchmark():
    # 1. Read data
    if not os.path.exists(CSV_PATH):
        print(f"Error: Could not find '{CSV_PATH}'. Make sure you are running this from the project root directory.")
        return
        
    df = pd.read_csv(CSV_PATH)
    
    # 2. Setup styles for a clean, modern dashboard aesthetic
    plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
    
    # Create 2 aligned subplots sharing the X-axis (Batch ID)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 8.5), sharex=True, dpi=150)
    
    # Harmony color palette
    colors = {
        'avg_e2e': '#0066cc',      # Royal Blue
        'min_e2e': '#2eca7f',      # Fresh Green
        'max_e2e': '#e83e8c',      # Pinkish Red
        'write_dur': '#fd7e14',    # Orange
        'trigger_dur': '#6c757d',  # Muted Gray
        'input_rate': '#6f42c1',   # Deep Purple
        'process_rate': '#dc3545'  # Crimson Red
    }
    
    # -------------------------------------------------------------
    # SUBPLOT 1: Latencies & Durations (Trễ & Thời gian xử lý)
    # -------------------------------------------------------------
    ax1.plot(df['batch_id'], df['avg_e2e_latency'], label='Avg E2E Latency (Trung bình)', 
             color=colors['avg_e2e'], linewidth=2.5, marker='o', markersize=4)
    ax1.plot(df['batch_id'], df['min_e2e_latency'], label='Min E2E Latency (Nhanh nhất)', 
             color=colors['min_e2e'], linewidth=1.5, linestyle='--')
    ax1.plot(df['batch_id'], df['max_e2e_latency'], label='Max E2E Latency (Chậm nhất)', 
             color=colors['max_e2e'], linewidth=1.5, linestyle='--')
    ax1.plot(df['batch_id'], df['write_duration'], label='Iceberg Commit Duration (Thời gian ghi)', 
             color=colors['write_dur'], linewidth=1.5, alpha=0.8)
    ax1.plot(df['batch_id'], df['trigger_duration'], label='Trigger Duration (Tổng thời gian Batch)', 
             color=colors['trigger_dur'], linewidth=1.2, linestyle=':', alpha=0.8)
    
    ax1.set_title("Spark Ingestion Latencies & Execution Durations per Batch", fontsize=13, fontweight='bold', pad=12)
    ax1.set_ylabel("Duration / Latency (seconds)", fontsize=10, fontweight='bold')
    ax1.legend(loc='upper right', frameon=True, facecolor='white', framealpha=0.9, fontsize=9)
    ax1.grid(True, linestyle=':', alpha=0.6)
    
    # Annotate the cold-start warm-up latency at Batch 0
    b0_trigger = df.loc[df['batch_id'] == 0, 'trigger_duration'].values[0]
    ax1.annotate(f'Cold-Start Warmup\n({b0_trigger:.2f}s)', 
                 xy=(0, b0_trigger), 
                 xytext=(2.5, b0_trigger - 1.5),
                 arrowprops=dict(facecolor='#495057', shrink=0.08, width=1, headwidth=6, headlength=6),
                 fontsize=9, fontweight='bold', color='#495057')

    # -------------------------------------------------------------
    # SUBPLOT 2: Throughput (Năng suất xử lý)
    # -------------------------------------------------------------
    ax2.plot(df['batch_id'], df['input_rate'], label='Ingestion Rate from Kafka (Dữ liệu vào)', 
             color=colors['input_rate'], linewidth=2, marker='s', markersize=3)
    ax2.plot(df['batch_id'], df['process_rate'], label='Spark Processing Capacity (Năng lực xử lý)', 
             color=colors['process_rate'], linewidth=2)
    
    # Plot Consumer Lag if it exists and has valid values (> 0)
    if 'kafka_lag' in df.columns and df['kafka_lag'].max() > 0:
        ax2_twin = ax2.twinx()
        ax2_twin.plot(df['batch_id'], df['kafka_lag'], label='Kafka Consumer Lag (Tồn đọng)', 
                      color='#dc3545', linewidth=1.5, linestyle='-.', alpha=0.7)
        ax2_twin.set_ylabel("Lag (records)", fontsize=10, color='#dc3545')
        ax2_twin.legend(loc='lower right')
        ax2_twin.grid(False)
        
    ax2.set_title("Data Ingestion Throughput vs. Spark Engine Capacity", fontsize=13, fontweight='bold', pad=12)
    ax2.set_xlabel("Batch ID (Số thứ tự lô xử lý)", fontsize=10, fontweight='bold')
    ax2.set_ylabel("Throughput (records/second)", fontsize=10, fontweight='bold')
    ax2.legend(loc='upper right', frameon=True, facecolor='white', framealpha=0.9, fontsize=9)
    ax2.grid(True, linestyle=':', alpha=0.6)
    
    # 3. Save layout
    plt.tight_layout()
    
    # Save the chart locally
    os.makedirs(os.path.dirname(OUTPUT_IMAGE), exist_ok=True)
    plt.savefig(OUTPUT_IMAGE, bbox_inches='tight', dpi=200)
    plt.close()
    
    print("\n" + "="*65)
    print("LINE CHART GENERATED SUCCESSFULLY!")
    print(f"Saved path: [benchmark_chart.png](file:///{os.path.abspath(OUTPUT_IMAGE).replace(chr(92), '/')})")
    print("="*65 + "\n")

if __name__ == "__main__":
    plot_benchmark()
