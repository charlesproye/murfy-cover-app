import { IconCopy, IconMailForward } from '@tabler/icons-react';
import React from 'react';
import { toast } from 'sonner';

const Help = (): React.ReactElement => {
  const emailAddress = 'contact@bib-batteries.fr';

  const handleCopyEmail = (): void => {
    navigator.clipboard.writeText(emailAddress);
    toast.success('Email address copied to clipboard');
  };

  return (
    <div className="flex items-center gap-2">
      <p className="text-gray dark:text-dark-gray text-sm font-thin min-w-14">Help</p>
      <div className="flex gap-4 items-center">
        <a
          href={`mailto:${emailAddress}`}
          className="cursor-pointer inline-flex items-center"
        >
          <IconMailForward
            size={20}
            stroke={1.7}
            color="gray"
            style={{ pointerEvents: 'none' }}
          />
        </a>
        <IconCopy
          size={20}
          stroke={1.7}
          color="gray"
          className="cursor-pointer"
          onClick={handleCopyEmail}
        />
      </div>
    </div>
  );
};

export default Help;
