import { NO_DATA } from '@/lib/staticData';

export const formatNumber = (
  value: string | number | null | undefined,
  unit?: string,
): string => {
  if (value === null || value === undefined) return NO_DATA;
  const num = typeof value === 'string' ? parseFloat(value.replace(/\s/g, '')) : value;
  return `${num.toLocaleString('en-US')}${unit ? ` ${unit}` : ''}`;
};

export const capitalizeFirstLetter = (val?: string | null) => {
  if (!val) return NO_DATA;
  return val.charAt(0).toUpperCase() + val.slice(1);
};

export const getPercentageVariation = (data: number[]): number | null => {
  let percentage = null;
  if (data.length >= 2) {
    const last = data[0];
    const secondLast = data[1];

    percentage = parseFloat(
      (((last - secondLast) / Math.abs(secondLast)) * 100).toFixed(2),
    );
  }
  return percentage;
};
