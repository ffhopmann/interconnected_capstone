# COMTRADE Data Pipeline

## Overview
This consolidated Jupyter notebook extracts global trade data from the UN COMTRADE API with advanced filtering capabilities and creates comprehensive data structures for analysis.

## Files
- `comtrade_data_pipeline.ipynb` - Main notebook with all functionality
- `data/` - Output directory for generated files

## API Key
✅ **Updated API Key**: `571b4cf309ba42c796184e39d697b53a`

## 🆕 New Features
- **HS Commodity Filtering**: Filter by any of 97 HS 2-digit commodity categories
- **Transport Mode Filtering**: Filter by specific transport modes (sea, air, road, etc.)
- **Dynamic File Naming**: Files automatically named based on selected filters
- **Organized Folder Structure**: Matrices and dictionaries stored in separate folders
- **HS Codes Mapping**: Complete mapping of all 97 HS commodity categories

## Generated Files

### 1. Country ID Mapping (`country_id_mapping.json`)
Maps country names to sequential IDs with World=0:
```json
{
  "World": 0,
  "Germany": 1,
  "USA": 2,
  "China": 3
}
```

### 2. Trade Dictionaries (`all_trade_dictionaries/`)
Dynamic naming: `trade_dictionary_{transport_mode}_{commodity}.json`
```json
{
  "Germany": {
    "USA": 175528388461.0,
    "China": 97733536224.0,
    "World": 1683517070397.0
  },
  "World": {
    "USA": 2871234567890.0,
    "World": 20064821609337.0
  }
}
```

### 3. Trade Matrices (`all_trade_matrices/`)
Dynamic naming: `trade_matrix_{transport_mode}_{commodity}.csv`
```
,World,Germany,USA,China
World,20064821609337.0,8686541942.0,2871234567890.0,2971234567890.0
Germany,1683517070397.0,0.0,175528388461.0,97733536224.0
USA,2063802611123.0,75381806630.0,0.0,143545716168.0
China,3576543293842.0,107059114975.0,525648764497.0,0.0
```

### 4. HS Codes Mapping (`hs_codes_mapping.json`)
Complete mapping of all 97 HS 2-digit commodity categories:
```json
{
  "TOTAL": "TOTAL",
  "01": "Live animals",
  "27": "Mineral fuels, mineral oils and products of their distillation",
  "84": "Machinery and mechanical appliances; parts thereof"
}
```

### 5. Metadata (`extraction_metadata_{transport}_{commodity}.json`)
Information about the extraction process, filters used, and results.

## Configuration

### Transport Mode Selection
In cell 6, set `SELECTED_TRANSPORT_MODE`:
- `'ALL'` - All transport modes (default)
- `'WATER_ONLY'` - Sea + Inland waterway + Combined water
- `'SEA_ONLY'` - Sea transport only
- `'AIR_ONLY'` - Air transport only
- `'ROAD_ONLY'` - Road transport only
- etc.

### HS Commodity Selection
In cell 6, set `SELECTED_HS_CODE`:
- `'TOTAL'` - All commodities (default)
- `'27'` - Mineral fuels, oils
- `'84'` - Machinery and mechanical appliances
- `'85'` - Electrical machinery and equipment
- Any 2-digit HS code from 01-97

### Example File Names
- All modes, all commodities: `trade_matrix_all_transport_modes_total_trade_volume.csv`
- Water transport, all commodities: `trade_matrix_water_only_total_trade_volume.csv`
- All modes, mineral fuels: `trade_matrix_all_transport_modes_HS27.csv`
- Sea transport, machinery: `trade_matrix_sea_only_HS84.csv`

## Usage

### Running the Notebook
1. Open `comtrade_data_pipeline.ipynb` in Jupyter
2. Configure transport mode and commodity in cell 6
3. Run all cells sequentially
4. Adjust `MAX_COUNTRIES` variable for testing vs full extraction:
   - `MAX_COUNTRIES = 10` - Test mode (10 countries)
   - `MAX_COUNTRIES = None` - Full extraction (all countries)

### Loading Data in Python
```python
import pandas as pd
import json

# Load specific matrix and dictionary (example for water transport, all commodities)
trade_matrix = pd.read_csv('data/all_trade_matrices/trade_matrix_water_only_total_trade_volume.csv', index_col=0)

with open('data/all_trade_dictionaries/trade_dictionary_water_only_total_trade_volume.json', 'r') as f:
    trade_dict = json.load(f)

# Load common mappings
with open('data/country_id_mapping.json', 'r') as f:
    country_ids = json.load(f)

with open('data/hs_codes_mapping.json', 'r') as f:
    hs_codes = json.load(f)

# Access data with World totals
print(f"World total: ${trade_matrix.loc['World', 'World']:,.0f}")
print(f"Germany exports: ${trade_matrix.loc['Germany', 'World']:,.0f}")
print(f"USA imports: ${trade_matrix.loc['World', 'USA']:,.0f}")
print(f"Germany to USA: ${trade_matrix.loc['Germany', 'USA']:,.0f}")
```

## Data Consistency
Both the dictionary and matrix contain identical data:
- `trade_dict['Germany']['USA']` == `trade_matrix.loc['Germany', 'USA']`
- World totals are accessible via 'World' key/index
- Country IDs: World=0, all others start at 1

## Troubleshooting

### SSL Certificate Issues
If you encounter SSL errors:
1. Try running in a different Python environment
2. Use VPN or different network
3. Update SSL certificates
4. The notebook will still demonstrate functionality with simulated data

### API Rate Limits
- The notebook includes 2-second delays between API calls
- For full extraction, expect 2-4 hours runtime
- Start with test mode (`MAX_COUNTRIES = 10`) first

## Expected Output
- **Test mode (10 countries)**: ~5 minutes, 10×10 matrix
- **Medium mode (50 countries)**: ~30 minutes, 50×50 matrix
- **Full extraction**: 2-4 hours, 200+×200+ matrix with all COMTRADE countries