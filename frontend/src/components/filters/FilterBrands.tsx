import React from 'react';
import TitleBox from '@/components/common/TitleBox';
import { FilterBrandsProps } from '@/interfaces/dashboard/home/ResponseApi';
import BrandSelector from '@/components/filters/BrandSelector';

export const FilterBrands = ({
  changeBrand,
  selectedBrand,
  brands,
  data,
}: FilterBrandsProps): React.ReactNode => {
  return (
    <div className="flex justify-between items-center">
      <TitleBox
        title="Vehicle degradation"
        titleSecondary="SoH vs. Mileage"
        dataDownload={data}
        downloadName={`soh-vs-mileage-${selectedBrand}`}
      />
      <BrandSelector selected={selectedBrand} setSelected={changeBrand} brands={brands} />
    </div>
  );
};
