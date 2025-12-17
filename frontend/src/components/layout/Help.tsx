import { IconCopy, IconMailForward } from '@tabler/icons-react';
import React from 'react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';

const Help = (): React.ReactElement => {
  const emailAddress = 'contact@bib-batteries.fr';

  const handleCopyEmail = async (): Promise<void> => {
    try {
      await navigator.clipboard.writeText(emailAddress);
      toast.success('Email address copied to clipboard');
    } catch (error: unknown) {
      console.error('Error copying email address:', error as Error);
      toast.error('Error copying email address');
    }
  };

  return (
    <div className="flex items-center gap-2">
      <p className="text-gray dark:text-dark-gray text-sm font-thin min-w-14">Help</p>
      <div className="flex gap-2 items-center">
        <Button variant="ghost" className="p-1 [&_svg]:size-5" asChild>
          <a href={`mailto:${emailAddress}`}>
            <IconMailForward color="gray" />
          </a>
        </Button>
        <Button variant="ghost" className="p-1 [&_svg]:size-5" onClick={handleCopyEmail}>
          <IconCopy color="gray" />
        </Button>
      </div>
    </div>
  );
};

export default Help;
