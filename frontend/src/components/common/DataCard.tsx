'use client';

import React from 'react';

import { IconArrowDown, IconArrowUp } from '@tabler/icons-react';
import { DatacardDisplayOption } from '@/interfaces/common/DataCard';

const DataCard: React.FC<DatacardDisplayOption> = ({
  formattedData,
  variation,
  title,
  getIcon,
  isScalarVariation = false,
  actionElement,
}) => {
  const percentageColor = `
    ${
      variation === null
        ? 'invisible'
        : variation === 0
          ? 'text-gray-light'
          : variation < 0
            ? 'text-red-price'
            : 'text-green-price'
    }
  `;

  return (
    <div className="flex flex-col justify-between bg-white  p-3 rounded-[10px] w-[240px] mobile:h-[128px] h-auto">
      <div className="flex items-center space-x-1">
        {getIcon}
        <h2 className="font-medium text-gray text-base"> {title} </h2>
      </div>

      <div className="flex gap-4 items-end justify-between">
        <div className="flex flex-col">
          <span className="font-bold text-2xl">{formattedData}</span>
          {variation !== null && (
            <div className={`${percentageColor} flex gap-1`}>
              <span className="text-sm ">
                {variation > 0 ? `+${variation}` : `${variation}`}{' '}
                {isScalarVariation ? '' : '%'}{' '}
                <i className="text-xs italic">vs last month</i>
              </span>
              {variation !== 0 && (
                <>
                  {variation < 0 ? (
                    <IconArrowDown className="w-[18px] h-[18px]" />
                  ) : (
                    <IconArrowUp className="w-[18px] h-[18px]" />
                  )}
                </>
              )}
            </div>
          )}
        </div>
        {actionElement}
      </div>
    </div>
  );
};

export default DataCard;
