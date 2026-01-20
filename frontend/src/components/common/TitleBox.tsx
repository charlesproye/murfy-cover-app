import React from 'react';
import { DataTitleBox } from '@/interfaces/common/DataTitleBox';

const TitleBox = ({
  title,
  titleSecondary,
}: DataTitleBox): React.ReactNode => {
  return (
    <div className="flex flex-col">
      <p className="text-sm text-gray-blue">{title}</p>
      <div className="flex items-center gap-2">
        <p className="text-base font-semibold text-black"> {titleSecondary} </p>
      </div>
    </div>
  );
};

export default TitleBox;
