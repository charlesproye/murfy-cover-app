const BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:4000/v1';

//http://localhost:4000/v1/auth/login
export const ROUTES = {
  LOGIN: `${BASE_URL}/auth/login`,
  LOGOUT: `${BASE_URL}/auth/logout`,
  REFRESH: `${BASE_URL}/auth/refresh`,

  // DASHBOARD
  GET_LAST_TIMESTAMP_WITH_DATA: `${BASE_URL}/dashboard/get_last_timestamp_with_data`,
  KPI: `${BASE_URL}/dashboard/kpis`,
  SEARCHBAR: `${BASE_URL}/dashboard/search`,
  FILTERS: `${BASE_URL}/dashboard/filters`,
  KPIHOME: `${BASE_URL}/dashboard/individual/kpis`,
  SOHRANGE: `${BASE_URL}/dashboard/individual/range_soh`,
  NEW_VEHICLES: `${BASE_URL}/dashboard/individual/new_vehicles`,
  GLOBAL_DASHBOARD: `${BASE_URL}/dashboard/global_table`,
  GLOBAL_SCATTER_BRANDS: `${BASE_URL}/dashboard/scatter_plot_brands`,
  GLOBAL_SCATTER_REGION: `${BASE_URL}/dashboard/scatter_plot_regions`,
  SEARCH_VEHICLE: `${BASE_URL}/dashboard/search`,
  TABLE_BRANDS: `${BASE_URL}/dashboard/individual/table_brand`,
  TABLE_EXTREMUM: `${BASE_URL}/dashboard/individual/get_extremum_soh`,
  TRENDLINE_BRANDS: `${BASE_URL}/dashboard/individual/trendline_brand`,
  BRANDS: `${BASE_URL}/dashboard/individual/brands`,
  GET_GROUP: `${BASE_URL}/dashboard/individual/get_soh_by_groups`,

  // PASSPORT
  GRAPHS: `${BASE_URL}/passport/graph`,
  KPIGRAPH: `${BASE_URL}/passport/kpis`,
  CHARGING_CYCLES: `${BASE_URL}/passport/charging-cycles`,
  INFO_VEHICLE: `${BASE_URL}/passport/infos`,
  ESTIMATED_RANGE: `${BASE_URL}/passport/estimated_range`,
  KPIS_ADDITIONAL: `${BASE_URL}/passport/kpis_additional`,
  PDF: `${BASE_URL}/passport/download_rapport`,
  PIN_VEHICLE: `${BASE_URL}/passport/pin_vehicle`,
  GET_PINNED_VEHICLE: `${BASE_URL}/passport/get_pinned_vehicle`,
  PRICE_FORECAST: `${BASE_URL}/passport/get_price_forecast`,

  // INDIVIDUAL
  PINNED_VEHICLE: `${BASE_URL}/individual/vehicles/pinned`,
  FAST_CHARGE_VEHICLE: `${BASE_URL}/individual/vehicles/fast-charge`,
  CONSUMPTION_VEHICLE: `${BASE_URL}/individual/vehicles/consumption`,

  // STATIC DATA
  MODELS_WITH_DATA: `${BASE_URL}/static_data/models-with-data`,
  ALL_MODELS_WITH_TRENDLINE: `${BASE_URL}/static_data/all-models-with-trendline`,

  // FLASH REPORT
  VIN_DECODER: `${BASE_URL}/flash_report/vin-decoder`,
  SEND_EMAIL: `${BASE_URL}/flash_report/send-email`,
  GENERATION_DATA: `${BASE_URL}/flash_report/generation-data`,

  // TESLA ACTIVATION
  TESLA_CREATE_USER: `${BASE_URL}/tesla/create-user`,
};

export type RouteKey = keyof typeof ROUTES;
