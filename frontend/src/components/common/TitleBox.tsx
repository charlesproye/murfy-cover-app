import React from 'react';
import Download from '@/components/common/Download';
import { DataTitleBox } from '@/interfaces/common/DataTitleBox';

const TitleBox = ({
  title,
  titleSecondary,
  dataDownload,
  downloadName,
}: DataTitleBox): React.ReactNode => {
  return (
    <div className="flex flex-col">
      <p className="text-sm text-gray-blue">{title}</p>
      <div className="flex items-center gap-2">
        <p className="text-base font-semibold text-black"> {titleSecondary} </p>
        {dataDownload && (
          <Download
            data={dataDownload}
            filename={downloadName}
            className="text-primary w-4"
          />
        )}
      </div>
    </div>
  );
};

export default TitleBox;
