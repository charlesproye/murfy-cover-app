interface Point {
  x: number;
  y: number;
}

export type { Point };

export interface TrendlineCoefficients {
  a: number;
  b: number;
  c: number;
}

export interface RegressionCalculator {
  a: number;
  b: number;
  c: number;
  tension: number;
  getTrendlineY: (x: number) => number;
}

export type TrendlineType = 'min' | 'max' | 'avg';

const MIN_POINTS_REQUIRED = 10;

// Coefficients par défaut en cas d'erreur de parsing
const DEFAULT_COEFFICIENTS: TrendlineCoefficients = {
  a: -0.035995764480017775,
  b: 1,
  c: 22828.562645003018,
};

export const parseTrendlineEquation = (equation: string): TrendlineCoefficients => {
  try {
    // Extraction des coefficients de l'équation (supporte la notation scientifique)
    // Pattern: nombre (peut être en notation scientifique) + nombre * np.log1p(x / nombre)
    const match = equation.match(
      /([\d.]+(?:[eE][-+]?\d+)?)\s*\+\s*([-+]?[\d.]+(?:[eE][-+]?\d+)?)\s*\*\s*np\.log1p\(x\s*\/\s*([\d.]+(?:[eE][-+]?\d+)?)\)/,
    );

    if (!match) {
      throw new Error("Format d'équation invalide");
    }
    // L'ordre dans l'expression régulière est: (b) + (a) * np.log1p(x / (c))
    const [, b, a, c] = match.map(Number);
    return { a, b, c };
  } catch (error) {
    console.error("Erreur lors du parsing de l'équation:", error);
    return DEFAULT_COEFFICIENTS;
  }
};

export const calculateTrendlineY = (
  x: number,
  coefficients: TrendlineCoefficients = DEFAULT_COEFFICIENTS,
): number => {
  const { a, b, c } = coefficients;
  const y = b + a * Math.log1p(x / c);
  const scaledY = y * 100;
  return Math.max(0, Math.min(b * 100, scaledY));
};

export const getTrendlinePoints = (
  minX: number,
  maxX: number,
  coefficients: TrendlineCoefficients = DEFAULT_COEFFICIENTS,
): Point[] => {
  const result: Point[] = [];
  const steps = 100;

  for (let i = 0; i <= steps; i++) {
    const x = minX + ((maxX - minX) * i) / steps;
    const y = calculateTrendlineY(x, coefficients);
    result.push({ x, y });
  }

  return result;
};

export const calculateLogarithmicRegression = (
  points: Array<[number, number]> | Array<Point>,
  globalMinX?: number,
  globalMaxX?: number,
  bypassMinPoints: boolean = false,
  coefficients?: TrendlineCoefficients,
): Array<Point> | RegressionCalculator => {
  // Vérifier si les points sont valides
  const validPoints = points.filter((point) => {
    const [x, y] = Array.isArray(point) ? point : [point.x, point.y];
    return x != null && y != null && !isNaN(x) && !isNaN(y);
  });

  if (
    !validPoints.length ||
    (!bypassMinPoints && validPoints.length < MIN_POINTS_REQUIRED)
  ) {
    return [];
  }

  const { a, b, c } = coefficients || DEFAULT_COEFFICIENTS;

  if (globalMinX === undefined || globalMaxX === undefined) {
    return {
      a,
      b,
      c,
      tension: 0.4,
      getTrendlineY: (x: number) => {
        const y = b + a * Math.log1p(x / c);
        const scaledY = y * 100;
        return Math.max(0, Math.min(100, scaledY));
      },
    };
  }

  return getTrendlinePoints(globalMinX, globalMaxX, coefficients);
};
