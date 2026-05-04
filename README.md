# wind-checker
**Every 1 year Asset Panda API token must be refreshed
**Pulls from Svannet API if data is available for weather, if not, it pulls open meteo data to get the windspeeds at the location

To get Asset Panda token go to: https://team-asset-panda.readme.io/reference/post_v3-session-token
1. Enter login credentials
2. Click "run" or "try it"
3. Update toke in Settings - secrets and variables - actions - ASSET_PANDA_TOKEN

Updates:

- Created 4/10/2025
- Updated Asset Panda API token 5/4/2025
- Updated with new .yml file to "keep script alive" -> Should solve the issue with action expiring or going to sleep every two months

