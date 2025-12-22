import React, { useState, useEffect, useMemo } from 'react';
import useGetTrendlineBrands from '@/hooks/dashboard/home/useGetTrendlineBrands';
import { FilterBrands } from '@/components/filters/FilterBrands';
import LoadingSmall from '@/components/common/loading/loadingSmall';
import { GraphTrendlineProps } from '@/interfaces/dashboard/home/ResponseApi';
import TrendlineChart from '@/components/charts/home/TrendlineChart';

export const GraphTrendline: React.FC<GraphTrendlineProps> = ({ fleet }) => {
  const [selectedBrand, setSelectedBrand] = useState<string>('');
  const { data, isLoading } = useGetTrendlineBrands(fleet, selectedBrand);

  const trendlineEquations = useMemo(
    () =>
      data?.brands.map((brand) => ({
        brand: brand,
        trendline: brand.trendline,
        trendline_max: brand.trendline_max,
        trendline_min: brand.trendline_min,
      })) ?? [],
    [data],
  );

  // Updating selected brand when fleet changes
  useEffect(() => {
    setSelectedBrand('');
  }, [fleet]);
  useEffect(() => {
    if (!isLoading && data && data.brands.length > 0 && !selectedBrand) {
      setSelectedBrand(data.brands[0].oem_id ?? '');
    }
  }, [isLoading, data, selectedBrand]);

  if (isLoading) return <LoadingSmall />;

  return (
    <div className="flex flex-col gap-4">
      <FilterBrands
        changeBrand={setSelectedBrand}
        selectedBrand={selectedBrand}
        brands={data ? data.brands : []}
        data={data ? data.trendline : []}
      />
      {data && data.trendline.length > 0 ? (
        <div className="relative w-full h-[400px]">
          <TrendlineChart
            data={data.trendline}
            trendlineEquations={trendlineEquations}
            selectedBrand={selectedBrand}
          />
          <p className="text-xs text-gray-500 text-center italic">Mileage (km)</p>
        </div>
      ) : (
        <p className="text-center text-sm text-gray"> No data available </p>
      )}
    </div>
  );
};
