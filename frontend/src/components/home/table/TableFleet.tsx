import LoadingSmall from '@/components/common/loading/loadingSmall';
import React from 'react';

import useInfiniteScroll from '@/hooks/dashboard/common/useInfiniteScroll';
import Link from 'next/link';
import useGetTableFleet from '@/hooks/dashboard/home/useGetTableFleet';
import { FleetDataItem } from '@/interfaces/dashboard/home/table/TableFleetResult';
import TitleBox from '@/components/common/TitleBox';
import { formatNumber, NO_DATA } from '@/utils/formatNumber';

type GraphSoHProps = {
  fleet: string | null;
  setIsDisplay: (isDisplay: boolean, clickedData: string | null) => void;
  label: string | null;
};

const TablesFleet: React.FC<GraphSoHProps> = ({ fleet, setIsDisplay, label }) => {
  const { setListRef, data, isLoading } = useInfiniteScroll<FleetDataItem>({
    fleet,
    label,
    fetchFunction: useGetTableFleet,
  });

  if (isLoading && !data) return <LoadingSmall />;
  return (
    <div
      ref={setListRef}
      className="w-full max-h-[378px] overflow-auto rounded-lg box-boder p-6"
    >
      <div className="flex justify-between items-center">
        <TitleBox
          title={'Fleet overview'}
          titleSecondary={'Real Time data'}
          dataDownload={data}
          downloadName={`fleet-overview-${label?.toLowerCase()}`}
        />
        <div
          className="flex gap-2 bg-white-ghost rounded-xl h-10 px-4 text-sm items-center cursor-pointer text-gray"
          onClick={() => setIsDisplay(false, null)}
        >
          Return
        </div>
      </div>
      <table className=" mt-4 min-w-full mobile:min-w-auto bg-white text-[14px] leading-4 border-spacing-y-14">
        <thead>
          <tr className="text-gray-light whitespace-nowrap">
            <th className="py-2 text-left font-normal">Make</th>
            <th className="py-2 text-left font-normal">VIN</th>
            <th className="py-2 text-left font-normal">SoH (%)</th>
            <th className="py-2 text-left font-normal">Mileage (km)</th>
          </tr>
        </thead>
        <tbody>
          {data &&
            data.map((row, index) => (
              <tr
                key={index}
                className={`${index === data.length - 1 ? '' : 'border-b border-b-gray-light'}`}
              >
                <td className="py-4 font-medium">
                  {row.oem_name
                    ? row.oem_name.charAt(0).toUpperCase() + row.oem_name.slice(1)
                    : ''}
                </td>
                <td className="py-4 font-medium">
                  <Link href={`/dashboard/passport/${row.vin}`}>
                    <span className="text-primary cursor-pointer">{row.vin}</span>
                  </Link>
                </td>
                <td className="py-4 font-medium">
                  {row.soh ? row.soh.toFixed(2) : NO_DATA}
                </td>
                <td className="py-4 font-medium">{formatNumber(row.odometer, 'km')}</td>
              </tr>
            ))}
        </tbody>
      </table>
    </div>
  );
};

export default TablesFleet;
