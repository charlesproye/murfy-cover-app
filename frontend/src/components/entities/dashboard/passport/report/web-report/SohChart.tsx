import React, { useMemo } from 'react';
import { InfoVehicleResult } from '@/interfaces/dashboard/passport/infoVehicle';
import { IconBattery2Filled } from '@tabler/icons-react';
import {
  Chart as ChartJS,
  LineElement,
  PointElement,
  LinearScale,
  Title,
  CategoryScale,
  Tooltip,
  Legend,
  TooltipItem,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import { calculateTrendlineY } from '@/lib/trendline';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
);

const PREDICTION_DISTANCE = 30000; // 30 000 km de prédiction

interface SohChartProps {
  reportData: InfoVehicleResult;
}

const SohChart: React.FC<SohChartProps> = ({ reportData }) => {
  const currentSoh = reportData.battery_info?.soh || 0;

  // Estimer le kilométrage actuel
  const estimatedOdometer = useMemo(() => {
    return reportData.vehicle_info?.odometer || 30000;
  }, [reportData.vehicle_info]);

  // Générer un seul point pour le SoH actuel plutôt qu'une série historique
  const currentVehiclePoint = useMemo(() => {
    return {
      odometer: estimatedOdometer,
      soh: currentSoh,
    };
  }, [estimatedOdometer, currentSoh]);

  // Points de la courbe initiale (de 0km au point actuel)
  const initialCurvePoints = useMemo(() => {
    const points = [];
    const steps = 20;

    // Calculer la valeur de la tendance au point actuel
    const trendValueAtCurrentPoint = calculateTrendlineY(currentVehiclePoint.odometer);

    // Calculer l'offset pour que la courbe passe exactement par le point actuel
    const offset = currentVehiclePoint.soh - trendValueAtCurrentPoint;

    // Générer les points depuis 0km jusqu'au point actuel
    for (let i = 0; i <= steps; i++) {
      const x = (i / steps) * currentVehiclePoint.odometer;

      // Pour x=0, la formule logarithmique donne une valeur selon la tendance, pas forcément 100%
      const y = calculateTrendlineY(x) + offset;

      points.push({ x, y });
    }

    // Assurer que le dernier point est exactement le point actuel
    points[points.length - 1] = {
      x: currentVehiclePoint.odometer,
      y: currentVehiclePoint.soh,
    };

    return points;
  }, [currentVehiclePoint]);

  // Points de la prédiction du véhicule
  const vehiclePredictionPoints = useMemo(() => {
    const points = [];
    const startOdometer = currentVehiclePoint.odometer;
    const endOdometer = startOdometer + PREDICTION_DISTANCE;
    const step = PREDICTION_DISTANCE / 20;

    // Commencer par le point actuel
    points.push({
      x: startOdometer,
      y: currentVehiclePoint.soh,
    });

    // Calculer l'offset à partir de la courbe tendance
    const trendValueAtCurrentPoint = calculateTrendlineY(startOdometer);
    const offset = currentVehiclePoint.soh - trendValueAtCurrentPoint;

    // Appliquer la tendance avec l'offset
    for (let x = startOdometer + step; x <= endOdometer; x += step) {
      points.push({
        x,
        y: calculateTrendlineY(x) + offset,
      });
    }

    return points;
  }, [currentVehiclePoint]);

  // Configuration du graphique
  const chartData = useMemo(
    () => ({
      datasets: [
        // Courbe historique (de 0,100 jusqu'au point actuel)
        {
          label: 'Historique SoH',
          data: initialCurvePoints,
          borderColor: '#4CAF50',
          backgroundColor: '#4CAF50',
          tension: 0.2,
          fill: false,
          pointRadius: (ctx: { dataIndex: number }) => {
            // Afficher uniquement le point actuel
            return ctx.dataIndex === initialCurvePoints.length - 1 ? 4 : 0;
          },
          pointHoverRadius: (ctx: { dataIndex: number }) => {
            return ctx.dataIndex === initialCurvePoints.length - 1 ? 6 : 0;
          },
        },
        // Prédiction du véhicule
        {
          label: 'Prédiction SoH',
          data: vehiclePredictionPoints,
          borderColor: '#4CAF50',
          borderWidth: 2,
          borderDash: [5, 5],
          tension: 0.2,
          fill: false,
          pointRadius: 0,
        },
      ],
    }),
    [initialCurvePoints, vehiclePredictionPoints],
  );

  // Calcul des échelles
  const chartScales = useMemo(() => {
    const allValues = [
      ...initialCurvePoints.map((p) => p.y),
      ...(vehiclePredictionPoints?.map((p) => p.y) || []),
    ];

    const maxValue = Math.min(110, Math.ceil(Math.max(...allValues, 100)));
    const minValue = Math.max(60, Math.floor(Math.min(...allValues) - 5));
    const range = maxValue - minValue;
    const stepSize = Math.ceil(range / 4);

    const maxOdometer = currentVehiclePoint.odometer + PREDICTION_DISTANCE;

    return {
      x: {
        type: 'linear' as const,
        ticks: {
          font: { size: 12 },
          callback: function (tickValue: number | string): string {
            const value =
              typeof tickValue === 'string' ? parseFloat(tickValue) : tickValue;
            return (value / 1000).toFixed(1) + 'K';
          },
        },
        grid: { display: false },
        border: { display: false },
        max: maxOdometer,
      },
      y: {
        beginAtZero: false,
        ticks: {
          stepSize: stepSize,
          font: { size: 12 },
          callback: function (tickValue: number | string): string {
            const value =
              typeof tickValue === 'string' ? parseFloat(tickValue) : tickValue;
            return Math.round(value) + '%';
          },
        },
        grid: { display: true },
        min: minValue,
        max: maxValue,
        border: { display: false },
      },
    };
  }, [initialCurvePoints, vehiclePredictionPoints, currentVehiclePoint]);

  // Options du graphique
  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      title: {
        display: false,
      },
      legend: {
        display: false,
      },
      tooltip: {
        enabled: true,
        backgroundColor: 'rgba(0, 0, 0, 0.8)',
        callbacks: {
          label: (context: TooltipItem<'line'>) => {
            const label = context.dataset.label || '';
            const value = context.parsed.y;
            return `${label}: ${value.toFixed(1)}%`;
          },
        },
      },
    },
    scales: chartScales,
  };

  return (
    <div className="rounded-xl mb-6 shadow-xs max-w-4xl w-full overflow-hidden">
      <div className="bg-white w-full p-4 md:p-6">
        <div className="flex items-center justify-between mb-5">
          <div className="flex items-center space-x-2">
            <div className="w-6 h-6 rounded-full flex items-center justify-center">
              <IconBattery2Filled stroke={1.5} className="text-green-rapport" />
            </div>
            <div>
              <h3 className="text-base font-medium">Vehicle overview</h3>
              <p className="text-sm text-gray">State-of-Health evolution</p>
            </div>
          </div>

          <div className="flex items-center gap-4">
            <span className="flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-green-rapport"></div>
              <p className="text-gray-blue text-sm">Vehicle SoH</p>
              <div className="w-4 h-[2px] border-t-2 border-dashed border-green-rapport"></div>
              <p className="text-gray-blue text-sm">Prediction</p>
            </span>
          </div>
        </div>

        <div className="flex flex-col">
          {/* Graphique SoH avec Chart.js */}
          <div className="h-64 w-full">
            <Line data={chartData} options={options} />
          </div>

          {/* Section d'informations */}
          <div className="mt-6 pt-4 border-t border-gray/20">
            <div className="text-xs text-gray-blue">
              <p>
                SoH represents the health of your battery compared to its new condition. A
                new battery has an SoH of 100%. Natural degradation reduces this
                percentage over time and charging cycles.
              </p>
              <p className="mt-2">
                An SoH of over 80% is considered good for a electric car. Below 70%, range
                can be significantly reduced.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SohChart;
