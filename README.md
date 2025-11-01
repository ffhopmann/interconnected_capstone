# Global Trade & Maritime Shipping Network Analysis

A comprehensive data pipeline and analysis framework for extracting, analyzing, and visualizing global trade flows and maritime shipping networks using UN COMTRADE data and Global Marine Traffic Density System (GMTDS) data.

## Overview

This project provides tools to:
- Extract bilateral trade data from the UN COMTRADE API
- Generate trade matrices for network analysis
- Visualize trade relationships between countries
- Extract and analyze maritime shipping lanes from density data
- Build network graphs of shipping routes and port connections

## Project Structure

### 1. `comtrade_data_pipeline.ipynb`
Redesigned COMTRADE data extraction pipeline that pulls bilateral trade data and generates trade matrices.

**Key Features:**
- Configurable country selection (specific countries or all 219+ valid countries)
- Multiple transport modes (SEA, AIR, ROAD, RAIL, PIPELINE, or ALL)
- HS commodity code filtering (TOTAL or specific 2-digit codes)
- Efficient API calls with error handling and rate limiting
- Automatic CSV matrix output with metadata

**Configuration Options:**
- `SELECTED_COUNTRIES`: List of country names or 'ALL'
- `TRANSPORT_MODE`: 'ALL', 'SEA_ONLY', 'AIR_ONLY', etc.
- `HS_CODE`: 'TOTAL' or specific code (e.g., 'HS72' for Iron and Steel)
- `PERIOD`: Year for data extraction (e.g., '2024')

**Output Format:**
- **Trade Matrix CSV**: Rows = Exporting countries, Columns = Importing countries
- **Values**: Bilateral export values in USD
- **Metadata JSON**: Extraction parameters and summary statistics

### 2. `comtrade_analysis.ipynb`
Analysis and visualization of COMTRADE trade data.

**Capabilities:**
- Load and analyze trade matrices
- Visualize bilateral trade flows between countries
- Create network graphs of top trading nations
- Calculate trade statistics (volume, density, sparsity)

**Visualizations Include:**
- Curved arrow diagrams showing trade direction and volume
- Network graphs with nodes sized by total trade volume
- Edge thickness representing bilateral trade intensity

**Example Analysis:**
- Germany, USA, and China trade relationships (2024)
- Top N countries by trade volume network visualization
- Matrix sparsity and connectivity analysis

### 3. `network_extraction.ipynb`
Maritime shipping network extraction from GMTDS density data.

**Key Features:**
- Load and aggregate shipping density data from multiple CSV files
- Spatial aggregation methods: K-means clustering, grid-based, or none
- Port data integration filtered by water body and harbor size
- Strategic choke point identification
- Network graph generation of shipping lanes and connections

**Configurable Parameters:**
- `AGGREGATION_METHOD`: 'kmeans', 'grid', or 'none'
- `KMEANS_N_CLUSTERS`: Number of clusters for spatial aggregation
- `MIN_VALUE_THRESHOLD`: Minimum density threshold for inclusion
- `CONNECTIONS_PER_NODE`: K-nearest neighbors for network building
- `WATER_BODIES`: Filter ports by region (Mediterranean, Black Sea, etc.)
- `HARBOR_SIZES`: Include ports by size (V, S, M, L)

**Data Processing:**
- Combines monthly shipping density data
- Removes duplicate locations across datasets
- Visualizes with log-scale color mapping
- Extracts shipping lane coordinates

## Setup

### Prerequisites

```bash
pip install pandas numpy matplotlib networkx comtradeapicall
pip install geopandas scikit-learn scipy
```

### API Key Configuration

Create a file named `COMTRADE_API_KEY.json` in the project directory:

```json
{
  "SUBSCRIPTION_KEY": "your_api_key_here"
}
```

Get your API key from: [UN COMTRADE API Registration](https://comtradeapi.un.org/)

## Usage

### Extract Trade Data

1. Open [comtrade_data_pipeline.ipynb](comtrade_data_pipeline.ipynb)
2. Configure settings in Section 1 (countries, transport mode, HS code, period)
3. Run all cells
4. Find output in `./data/all_trade_matrices/`

### Analyze Trade Networks

1. Open [comtrade_analysis.ipynb](comtrade_analysis.ipynb)
2. Load a trade matrix CSV from the pipeline output
3. Run analysis cells to generate visualizations
4. Adjust `TOP_N_COUNTRIES` parameter for network size

### Extract Shipping Networks

1. Place GMTDS density CSV files in `./density_data/` organized by subfolders
2. Open [network_extraction.ipynb](network_extraction.ipynb)
3. Configure aggregation parameters
4. Run all cells to generate shipping lane networks

## Data Sources

- **Trade Data**: [UN COMTRADE API](https://comtradeapi.un.org/)
  - Country Codes: [Reporters Reference](https://comtradeapi.un.org/files/v1/app/reference/Reporters.json)
  - Documentation: [COMTRADE Wiki](https://unstats.un.org/wiki/display/comtrade/)

- **Shipping Density**: Global Marine Traffic Density System (GMTDS)
  - Resolution: 20 meters
  - Coverage: 2023-2024

## Output Examples

### Trade Matrices
- Matrix dimensions: 220x220 (World + 219 countries)
- Format: CSV with country names as index/columns
- Values: USD trade volumes

### Network Graphs
- Nodes: Countries or shipping waypoints
- Edges: Trade flows or shipping routes
- Attributes: Volume, distance, frequency

### Visualizations
- Bilateral trade flow diagrams
- Circular network layouts
- Geographic shipping density maps

## Notes

- **API Rate Limits**: ~0.2s delay between calls
- **Large Datasets**: Processing 200+ countries may take 10+ minutes
- **Matrix Sparsity**: ~62% for global 'ALL' transport mode
- **Data Completeness**: Most recent year may have incomplete data

## Common HS Codes

- `TOTAL`: All commodities
- `HS10`: Cereals
- `HS27`: Mineral Fuels
- `HS72`: Iron and Steel
- `HS89`: Ships and Boats

## Transport Mode Codes

- `ALL`: All transport modes (single aggregated API call)
- `SEA_ONLY`: Maritime shipping (code 2100)
- `AIR_ONLY`: Air freight (code 1000)
- `ROAD_ONLY`: Road transport (code 3200)
- `RAIL_ONLY`: Rail transport (code 3100)
- `PIPELINE_ONLY`: Pipeline (code 9100)

## License

This project uses data from UN COMTRADE, which is freely available for research purposes.

## Contact

For questions about the data pipeline or analysis methods, please refer to the UN COMTRADE documentation or open an issue in this repository.
