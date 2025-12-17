import React from 'react';
import DownloadExtremum from '@/components/entities/dashboard/home/extremumSection/DownloadExtremum';
import { DataTitleBoxExtremum } from '@/interfaces/common/DataTitleBox';

const TitleBoxExtremum = ({
  title,
  titleSecondary,
  fleet,
  quality,
  downloadName,
}: DataTitleBoxExtremum): React.ReactNode => {
  return (
    <div className="flex flex-col">
      <p className="text-sm text-gray-blue">{title}</p>
      <div className="flex items-center gap-2">
        <p className="text-base font-semibold text-black"> {titleSecondary} </p>
        {fleet && (
          <DownloadExtremum
            fleet={fleet}
            quality={quality}
            filename={downloadName}
            className="text-primary w-4"
          />
        )}
      </div>
    </div>
  );
};

export default TitleBoxExtremum;
