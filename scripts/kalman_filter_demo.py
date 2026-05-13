import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# read data
with open('sample.json', 'r') as file:
    raw_data = json.load(file)

waypoints = [item['msgBusWayPoint'] for item in raw_data if 'msgBusWayPoint' in item]
df = pd.DataFrame(waypoints)

# R matrix
print("Calculating measurement noise variance (R) from stationary vehicle data...")

# store variances of vehicles that qualify as stationary
valid_variances_x = []
valid_variances_y = []
# dictionary to store the variance for each vehicle
vehicle_R_dict = {}

# group data by vehicle
grouped = df.groupby('vehicle')

for vehicle_id, group in grouped:
    # sort by time
    group = group.sort_values('datetime').reset_index(drop=True)
    
    # find consecutive segments where speed
    is_zero = (group['speed'] == 0)
    # create group IDs for consecutive ranges having speed = 0
    streaks = (is_zero != is_zero.shift()).cumsum()
    zero_segments = group[is_zero].groupby(streaks)
    
    var_x_list = []
    var_y_list = []
    
    for _, segment in zero_segments:
        # only consider stationary segments with 10 or more samples to calculate variance
        if len(segment) >= 10:
            var_x_list.append(segment['x'].var())
            var_y_list.append(segment['y'].var())
            
    # if this vehicle has a stationary segment satisfying the condition
    if var_x_list and var_y_list:
        mean_var_x = np.mean(var_x_list)
        mean_var_y = np.mean(var_y_list)
        vehicle_R_dict[vehicle_id] = (mean_var_x, mean_var_y)
        valid_variances_x.append(mean_var_x)
        valid_variances_y.append(mean_var_y)

# calculate the global mean as a fallback (contingency)
# use max() to avoid calculating variance = 0, which causes an inverse matrix error
global_var_x = max(np.mean(valid_variances_x) if valid_variances_x else 1e-10, 1e-10)
global_var_y = max(np.mean(valid_variances_y) if valid_variances_y else 1e-10, 1e-10)

print(f"Complete! Contingent variance: X={global_var_x:.2e}, Y={global_var_y:.2e}")

# Kalman filter

# select two vehicles with the most data to use as illustrative examples
top_2_vehicles = df['vehicle'].value_counts().index[:2].tolist()
# color palette for the two vehicles
colors = ['red', 'blue']

plt.figure(figsize=(14, 8))

# fixed parameters
H = np.array([[1, 0, 0, 0], 
              [0, 1, 0, 0]])
I = np.eye(4)
# square the acceleration standard deviation for Q matrix
sigma_a_sq = 1.96e-10

# iterate through each vehicle to filter and draw
for idx, target_vehicle in enumerate(top_2_vehicles):
    bus_df = df[df['vehicle'] == target_vehicle].copy()
    bus_df = bus_df.sort_values(by='datetime').reset_index(drop=True)
    
    raw_x = bus_df['x'].values
    raw_y = bus_df['y'].values
    times = bus_df['datetime'].values
    
    # get the vehicle's own R, if not available then get contigent R
    if target_vehicle in vehicle_R_dict:
        var_x, var_y = vehicle_R_dict[target_vehicle]
        # avoid cases where variance = 0
        var_x, var_y = max(var_x, 1e-10), max(var_y, 1e-10)
    else:
        var_x, var_y = global_var_x, global_var_y
        
    R = np.array([[var_x, 0], 
                  [0, var_y]])
    
    filtered_x = []
    filtered_y = []
    
    x_hat = np.array([[raw_x[0]], [raw_y[0]], [0], [0]])
    P = np.array([[1, 0, 0, 0],
                  [0, 1, 0, 0],
                  [0, 0, 1, 0],
                  [0, 0, 0, 1]])
    
    for i in range(len(raw_x)):
        dt = times[i] - times[i-1] if i > 0 else 1.0
        # avoid the case where dt = 0 due to 2 packets being sent at the same time
        if dt <= 0: dt = 1.0 
        
        F = np.array([[1, 0, dt, 0],
                      [0, 1, 0, dt],
                      [0, 0, 1, 0],
                      [0, 0, 0, 1]])
        
        # Q matrix (G * Qa * G^T)
        dt2 = dt**2
        dt3 = dt**3
        dt4 = dt**4
        Q = np.array([
            [(dt4/4)*sigma_a_sq, 0,                  (dt3/2)*sigma_a_sq, 0                 ],
            [0,                  (dt4/4)*sigma_a_sq, 0,                  (dt3/2)*sigma_a_sq],
            [(dt3/2)*sigma_a_sq, 0,                  dt2*sigma_a_sq,     0                 ],
            [0,                  (dt3/2)*sigma_a_sq, 0,                  dt2*sigma_a_sq    ]
        ])
        
        # predict
        x_hat = F @ x_hat
        P = F @ P @ F.T + Q
        
        # update
        Z = np.array([[raw_x[i]], [raw_y[i]]])
        Y = Z - (H @ x_hat)
        S = (H @ P @ H.T) + R
        K = P @ H.T @ np.linalg.inv(S)
        
        x_hat = x_hat + (K @ Y)
        P = (I - (K @ H)) @ P
        
        filtered_x.append(x_hat[0, 0])
        filtered_y.append(x_hat[1, 0])
        
    # draw chart
    c = colors[idx]
    v_id_short = target_vehicle[:6]
    # draw raw line: Dashed line (linestyle=':'), marker='.', faint (alpha=0.4)
    plt.plot(raw_x, raw_y, color=c, linestyle=':', marker='.', alpha=0.4, 
             label=f'Xe {v_id_short} - Thô')
    # draw filtered line: Solid line (linestyle='-'), no marker, bolder (alpha=0.9), thick line (linewidth=2)
    plt.plot(filtered_x, filtered_y, color=c, linestyle='-', alpha=0.9, linewidth=2, 
             label=f'Xe {v_id_short} - Lọc')

# set up display
plt.title('Chart')
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.legend()
plt.grid(True)
plt.show()