export type BarChartProps = {
  data: { lower_bound: number; upper_bound: number; vehicle_count: number }[];
};

export type HorizontalBarChartProps = {
  data: {
    period: string;
    vehicle_count: number;
  }[];
  period: 'Monthly' | 'Annually';
};
