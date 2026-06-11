import pandas as pd
import matplotlib.pyplot as plt
import os

CSV_PATH = "scripts/benchmark_results.csv"
OUTPUT_IMAGE = r"C:\Users\Lenovo\.gemini\antigravity-ide\brain\f15459cb-636a-44a1-a5c8-72516dabbf4b\benchmark_chart.png"

def main():
    if not os.path.exists(CSV_PATH):
        print(f"Error: {CSV_PATH} does not exist.")
        return
        
    df = pd.read_csv(CSV_PATH)
    
    # Filter out warmup batches (where is_warmup == 1) for stable statistics
    # but let's keep them if we want to show the initial spark compilation lag.
    # We can plot all of them to visualize the warm-up effect!
    
    # Set up matplotlib style for a premium look
    plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), dpi=150)
    
    # Color palette
    colors = {
        'avg_e2e': '#0066cc', # Blue
        'min_e2e': '#00cc66', # Green
        'max_e2e': '#cc0066', # Pink
        'write_dur': '#ff9900', # Orange
        'trigger_dur': '#777777', # Gray
        'input_rate': '#6f42c1', # Purple
        'process_rate': '#fd7e14' # Dark Orange
    }
    
    # Plot 1: Latency & Duration Metrics
    ax1.plot(df['batch_id'], df['avg_e2e_latency'], label='Avg E2E Latency', color=colors['avg_e2e'], linewidth=2, marker='o', markersize=4)
    ax1.plot(df['batch_id'], df['min_e2e_latency'], label='Min E2E Latency (Newest Record)', color=colors['min_e2e'], linewidth=1.5, linestyle='--')
    ax1.plot(df['batch_id'], df['max_e2e_latency'], label='Max E2E Latency (Oldest Record)', color=colors['max_e2e'], linewidth=1.5, linestyle='--')
    ax1.plot(df['batch_id'], df['write_duration'], label='Iceberg Write Duration', color=colors['write_dur'], linewidth=1.5)
    ax1.plot(df['batch_id'], df['trigger_duration'], label='Trigger Duration', color=colors['trigger_dur'], linewidth=1, linestyle=':')
    
    ax1.set_title("E2E Processing Latencies & Stage Durations per Batch", fontsize=12, fontweight='bold', pad=10)
    ax1.set_ylabel("Time (seconds)", fontsize=10)
    ax1.legend(loc='upper right', frameon=True, facecolor='white', framealpha=0.9)
    ax1.grid(True, linestyle=':', alpha=0.6)
    
    # Highlight Batch 0 warm-up execution lag
    warmup_duration = df.loc[df['batch_id'] == 0, 'trigger_duration'].values[0]
    ax1.annotate(f'Warm-up compilation lag\n({warmup_duration:.2f}s)', 
                 xy=(0, warmup_duration), 
                 xytext=(3, warmup_duration - 1.5),
                 arrowprops=dict(facecolor='black', shrink=0.05, width=1, headwidth=6))
    
    # Plot 2: Ingestion & Processing Throughput
    ax2.plot(df['batch_id'], df['input_rate'], label='Ingestion Rate (Kafka)', color=colors['input_rate'], linewidth=2, marker='s', markersize=3)
    ax2.plot(df['batch_id'], df['process_rate'], label='Spark Processing Capacity', color=colors['process_rate'], linewidth=2)
    
    ax2.set_title("Ingestion Throughput vs. Spark Processing Capacity", fontsize=12, fontweight='bold', pad=10)
    ax2.set_xlabel("Batch ID", fontsize=10)
    ax2.set_ylabel("Throughput (records/second)", fontsize=10)
    ax2.legend(loc='upper right', frameon=True, facecolor='white', framealpha=0.9)
    ax2.grid(True, linestyle=':', alpha=0.6)
    
    plt.tight_layout()
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(OUTPUT_IMAGE), exist_ok=True)
    plt.savefig(OUTPUT_IMAGE, bbox_inches='tight')
    plt.close()
    
    print(f"Benchmark chart saved successfully to {OUTPUT_IMAGE}")

if __name__ == "__main__":
    main()
