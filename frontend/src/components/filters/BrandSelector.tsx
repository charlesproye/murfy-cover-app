import { TableExtremumResult } from '@/interfaces/dashboard/home/table/TablebrandResult';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import React from 'react';
import { capitalizeFirstLetter } from '@/lib/dataDisplay';

const BrandSelector: React.FC<{
  selected: string;
  setSelected: (value: string) => void;
  brands: TableExtremumResult['brands'];
  className?: string;
}> = ({ selected, setSelected, brands, className }) => {
  return (
    <Select onValueChange={(value) => setSelected(value)} value={selected}>
      <SelectTrigger className={`border-none focus:ring-0 w-[150px] ${className}`}>
        <SelectValue placeholder="Select a brand" />
      </SelectTrigger>
      <SelectContent>
        {brands?.map((brand) => (
          <SelectItem key={brand.oem_id} value={brand.oem_id}>
            {capitalizeFirstLetter(brand.oem_name)}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
};

export default BrandSelector;
