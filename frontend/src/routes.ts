const BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:4000/v1';

//http://localhost:4000/v1/auth/login
export const ROUTES = {
  LOGIN: `${BASE_URL}/auth/login`,
  LOGOUT: `${BASE_URL}/auth/logout`,
  REFRESH: `${BASE_URL}/auth/refresh`,

  // DASHBOARD
  LAST_TIMESTAMP_WITH_DATA: `${BASE_URL}/dashboard/last_timestamp_with_data`,
  SEARCHBAR: `${BASE_URL}/dashboard/search`,
  KPIHOME: `${BASE_URL}/dashboard/individual/kpis`,
  SOHRANGE: `${BASE_URL}/dashboard/individual/range_soh`,
  NEW_VEHICLES: `${BASE_URL}/dashboard/individual/new_vehicles`,
  SEARCH_VEHICLE: `${BASE_URL}/dashboard/search`,
  TABLE_BRANDS: `${BASE_URL}/dashboard/individual/table_brand`,
  TABLE_EXTREMUM: `${BASE_URL}/dashboard/individual/extremum_vehicles`,
  TRENDLINE_BRANDS: `${BASE_URL}/dashboard/individual/trendline_brand`,
  SOH_BY_GROUPS: `${BASE_URL}/dashboard/individual/soh_by_groups`,

  // PASSPORT
  GRAPHS: `${BASE_URL}/passport/graph`,
  KPIGRAPH: `${BASE_URL}/passport/kpis`,
  CHARGING_CYCLES: `${BASE_URL}/passport/charging-cycles`,
  INFO_VEHICLE: `${BASE_URL}/passport/infos`,
  ESTIMATED_RANGE: `${BASE_URL}/passport/estimated_range`,
  KPIS_ADDITIONAL: `${BASE_URL}/passport/kpis_additional`,
  PDF: `${BASE_URL}/passport/download_rapport`,
  PIN_VEHICLE: `${BASE_URL}/passport/pin_vehicle`,
  GET_PINNED_VEHICLE: `${BASE_URL}/passport/pinned_vehicle`,
  PRICE_FORECAST: `${BASE_URL}/passport/price_forecast`,
  IS_VIN_IN_FLEETS: `${BASE_URL}/passport/is_vin_in_fleets`,

  // INDIVIDUAL
  PINNED_VEHICLES: `${BASE_URL}/individual/vehicles/pinned`,

  // STATIC DATA
  MODELS_WITH_DATA: `${BASE_URL}/static_data/models-with-data`,

  // FLASH REPORT
  VIN_DECODER: `${BASE_URL}/flash_report/vin-decoder`,
  SEND_EMAIL: `${BASE_URL}/flash_report/send-email`,
  GENERATION_DATA: `${BASE_URL}/flash_report/generation-data`,
  ALL_MODELS_WITH_TRENDLINE: `${BASE_URL}/flash_report/models-with-trendline`,

  // TESLA ACTIVATION
  TESLA_CREATE_USER: `${BASE_URL}/tesla/create-user`,
};

export type RouteKey = keyof typeof ROUTES;
