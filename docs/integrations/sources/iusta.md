# Iusta Source Connector

## Overview
The Iusta Source Connector allows you to integrate with the Iusta API to import data.

## Configuration
- **IUSTA_AUTH_TOKEN**: Your authentication token for accessing the Iusta API.
- **Base URL**: `https://api.chevalier.iusta.io`

## Endpoints
- **Cases**: `/api/Cases`
- **Extended Search**: `/api/Cases/extendedSearch`
- **Custom Fields**: `/api/CustomFields`
- **Users**: `/api/XUsers`
- **Datasets**: `/api/Datasets`
- **Case Export**: `/api/Exports/v2/case/{case_id}`
- **Multi Dataset**: `/api/Datasets/{dataset_id}/CustomFieldVal`

## Usage
1. Configure the connector with your Iusta API credentials.
2. Use the provided endpoints to fetch data from Iusta.

## Changelog

### 0.1.0
- Initial release of the Iusta Source Connector.