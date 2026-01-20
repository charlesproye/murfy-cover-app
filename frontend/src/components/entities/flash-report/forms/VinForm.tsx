'use client';

import React, { useState } from 'react';
import { FormProvider, useForm } from 'react-hook-form';
import { toast } from 'sonner';
import { zodResolver } from '@hookform/resolvers/zod';

import { Button } from '@/components/ui/button';
import { IconArrowRight } from '@tabler/icons-react';
import { ROUTES } from '@/routes';

import {
  vinDecoderSchema,
  VinDecoder,
} from '@/components/entities/flash-report/forms/schema';
import { StringFormInput } from '@/components/form/StringFormInput';
import { useRouter } from 'next/navigation';
import fetchWithAuth from '@/services/fetchWithAuth';

export interface VinDecoderTypeVersion {
  type: string;
  version: string;
}

export interface VinDecoderResponse {
  has_trendline: boolean;
  has_trendline_bib: boolean;
  has_trendline_oem: boolean;
  make: string | null;
  model: string | null;
  type_version_list: VinDecoderTypeVersion[] | null;
}

export const VinForm = (): React.ReactElement => {
  const router = useRouter();
  const [isLoading, setIsLoading] = useState(false);

  const formControls = useForm({
    resolver: zodResolver(vinDecoderSchema),
    defaultValues: { vin: '' },
  });

  const onSubmit = async (data: VinDecoder) => {
    setIsLoading(true);
    try {
      const response = await fetchWithAuth<VinDecoderResponse>(ROUTES.VIN_DECODER, {
        method: 'POST',
        body: JSON.stringify(data.vin.trim()),
      });
      if (!response) {
        toast.error('Error decoding VIN');
      } else {
        // Store the full response data along with VIN in localStorage
        const flashReportData = {
          vin: data.vin.trim(),
          ...response,
        };
        localStorage.setItem('flash-report-data', JSON.stringify(flashReportData));
        router.push('/flash-report/step2');
      }
    } catch (error) {
      toast.error('Error decoding VIN: ' + (error as Error).message);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <FormProvider {...formControls}>
      <form onSubmit={formControls.handleSubmit(onSubmit)} name={'vinForm'}>
        <div className="flex flex-col gap-8 w-1/2 mx-auto">
          <div className="flex flex-col w-full gap-2">
            <p className="text-gray">Provide you VIN </p>
          </div>

          <StringFormInput
            name="vin"
            label="VIN"
            placeholder="Enter your VIN"
            required
            className=""
          />

          <div className="flex w-full justify-end">
            <Button type="submit" className="px-6" loading={isLoading}>
              Send <IconArrowRight size={17} />
            </Button>
          </div>
        </div>
      </form>
    </FormProvider>
  );
};
