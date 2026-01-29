import datetime
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.tsa.stattools import adfuller, coint
from hurst import compute_Hc

# Define start date and products to fetch data for
dte = datetime.date.today() - datetime.timedelta(days=120)
products = {
    'sugar': 'SB',
    'cocoa': 'CC',
    'coffee': 'KC',
    'wheat': 'KE',
    'soybean_oil': 'ZL',
    'soybean_meal': 'ZM',
    'corn': 'ZC',
    'orange_juice': 'OJ',
    'lumber': 'LBR',
    'oat': 'ZO',
    'cotton': 'CT',
    'rice': 'ZR'
}

# Fetch historical data for assets
dfs = []
for name, tick in products.items():
    print(name + ' ' + tick)
    data = yf.download(tick + '=F', dte.strftime('%Y-%m-%d'))[['Close']]
    data.rename(columns={'Close': name}, inplace=True)
    dfs.append(data)

# Collect into DataFrame
df = pd.concat(dfs, 1)[:-1]

# Solve NaN's
df.dropna(1, 'all', inplace=True)
df.ffill(inplace=True)
df.bfill(inplace=True)

# Create a correlation matrix to plot and analyse
corr_matrix = df.corr()
sns.heatmap(corr_matrix, annot=True)
plt.show()

# I want to know which products are uncorrelated, as these would be strong candidates for momentum
sorted_corr_matrix = corr_matrix.unstack().sort_values().reset_index()
sorted_corr_matrix.columns = ['x', 'y', 'corr']

uncorrelated_pairs = sorted_corr_matrix[
    (abs(sorted_corr_matrix['corr']) > -0.25) & (abs(sorted_corr_matrix['corr']) < 0.25)
]
uncorrelated_pairs.drop_duplicates('corr', inplace=True)
print(uncorrelated_pairs)

# Test a product's time series for stationarity
prod = df.iloc[:, 0]

stationarity_p_value = adfuller(prod)
if adfuller(prod)[1] < 0.10:
    print(f"Likely stationary: P-Value = {str(round(stationarity_p_value[1], 5))}")
else:
    print(f"Likely NOT stationary: P-Value = {str(round(stationarity_p_value[1], 5))}!")

# Evaluate Hurst equation
H, c, data = compute_Hc(prod, kind='price', simplified=True)

# Plot
f, ax = plt.subplots()
ax.plot(data[0], c*data[0]**H, color="deepskyblue")
ax.scatter(data[0], data[1], color="purple")
ax.set_xscale('log')
ax.set_yscale('log')
ax.set_xlabel('Time interval')
ax.set_ylabel('R/S ratio')
ax.grid(True)
plt.show()

print("H={:.4f}, c={:.4f}".format(H,c))
