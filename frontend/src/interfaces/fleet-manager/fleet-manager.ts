export type FleetInfo = {
  fleet_id: string;
  fleet_name: string;
};

export type MakeInfo = {
  make_id: string;
  make_name: string;
  make_conditions: string | null;
  soh_readout: boolean;
  soh_bib: boolean;
};

export type MakeInfoResponse = {
  makes: MakeInfo[];
};

export type ModelInfo = {
  model_id: string;
  model_name: string;
  type: string | null;
  version: string | null;
  battery_capacity: number | null;
};

export type ModelInfoResponse = {
  models: ModelInfo[];
};

export type VehicleStatus = {
  vin: string;
  requested_soh_bib: boolean;
  requested_soh_readout: boolean;
  requested_activation: boolean;
  status: boolean | null;
  message: string;
  comment: string | null;
};

export type VehicleInfo = {
  vin: string;
  make: string;
  model: string;
  type?: string | null;
};

export type ActivationInfo = {
  request_soh_bib: boolean;
  request_soh_oem: boolean;
  start_date?: string | null;
  end_date?: string | null;
};

export type ActivationOrder = {
  vehicle: VehicleInfo;
  activation: ActivationInfo;
  comment?: string | null;
};

export type ActivationRequest = {
  fleet_id: string;
  activation_orders: ActivationOrder[];
};

export type DeactivationRequest = {
  fleet_id: string;
  vins: string[];
};

export type ActivationResponse = {
  vehicles: VehicleStatus[];
};

export type FleetsResponse = {
  fleets: FleetInfo[];
};

export type VehicleStatusParams = {
  fleet_id: string;
  vin?: string;
};
