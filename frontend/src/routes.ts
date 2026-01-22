const BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:4000/v1';

//http://localhost:4000/v1/auth/login
export const ROUTES = {
  LOGIN: `${BASE_URL}/auth/login`,
  LOGOUT: `${BASE_URL}/auth/logout`,
  REFRESH: `${BASE_URL}/auth/refresh`,

  // DASHBOARD
  LAST_TIMESTAMP_WITH_DATA: `${BASE_URL}/dashboard/last_timestamp_with_data`,
  SEARCHBAR: `${BASE_URL}/dashboard/search`,
  KPIHOME: `${BASE_URL}/dashboard/kpis`,
  NEW_VEHICLES: `${BASE_URL}/dashboard/new_vehicles`,
  SEARCH_VEHICLE: `${BASE_URL}/dashboard/search`,
  TABLE_BRANDS: `${BASE_URL}/dashboard/table_brand`,
  TABLE_EXTREMUM: `${BASE_URL}/dashboard/extremum_vehicles`,
  SOH_BY_GROUPS: `${BASE_URL}/dashboard/soh_by_groups`,

  // PASSPORT
  GRAPHS: `${BASE_URL}/passport/graph`,
  INFO_VEHICLE: `${BASE_URL}/passport/infos`,
  PIN_VEHICLE: `${BASE_URL}/passport/pin_vehicle`,
  GET_PINNED_VEHICLE: `${BASE_URL}/passport/pinned_vehicle`,
  PRICE_FORECAST: `${BASE_URL}/passport/price_forecast`,
  IS_VIN_IN_FLEETS: `${BASE_URL}/passport/is_vin_in_fleets`,

  // PREMIUM
  PREMIUM_REPORT_HTML: `${BASE_URL}/premium`,
  PREMIUM_REPORT_PDF_SYNC: `${BASE_URL}/premium`,

  // READOUT
  READOUT_REPORT_HTML: `${BASE_URL}/readout`,

  // FAVORITES
  PINNED_VEHICLES: `${BASE_URL}/individual/favorite_table`,

  // STATIC DATA
  MODELS_WITH_DATA: `${BASE_URL}/static_data/models-with-data`,

  // FLASH REPORT
  VIN_DECODER: `${BASE_URL}/flash_report/vin-decoder`,
  SEND_REPORT_GENERATION: `${BASE_URL}/flash_report/send-report-generation`,
  GENERATION_DATA: `${BASE_URL}/flash_report/generation-data`,
  ALL_MODELS_WITH_TRENDLINE: `${BASE_URL}/flash_report/models-with-trendline`,

  // TESLA ACTIVATION
  TESLA_CREATE_USER: `${BASE_URL}/tesla/create-user`,

  // REPORT VERIFICATION
  VERIFY_REPORT: `${BASE_URL}/verify-report`,

  // FLEET MANAGER / VEHICLE ACTIVATION
  VEHICLE_FLEETS: `${BASE_URL}/account/fleets`,
  VEHICLE_MAKES: `${BASE_URL}/vehicle-command/makes`,
  VEHICLE_MODELS: (makeId: string) => `${BASE_URL}/vehicle-command/${makeId}/models`,
  VEHICLE_STATUS: `${BASE_URL}/account/vehicles`,
  VEHICLE_ACTIVATE: `${BASE_URL}/vehicle-command/activate`,
  VEHICLE_DEACTIVATE: `${BASE_URL}/vehicle-command/deactivate`,
};

export type RouteKey = keyof typeof ROUTES;
