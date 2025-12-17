'use client';

import React, { useState, useEffect } from 'react';
import { FormProvider, useForm } from 'react-hook-form';
import { toast } from 'sonner';
import { zodResolver } from '@hookform/resolvers/zod';

import { Button } from '@/components/ui/button';
import { IconArrowRight } from '@tabler/icons-react';
import { ROUTES } from '@/routes';

import {
  teslaActivationSchema,
  TeslaActivationType,
} from '@/components/entities/tesla-activation/forms/schema';
import { StringFormInput } from '@/components/form/StringFormInput';
import { useAuth } from '@/contexts/AuthContext';
import fetchWithAuth from '@/services/fetchWithAuth';

export const TeslaActivationForm = (): React.ReactElement => {
  const { user } = useAuth();
  const [isLoading, setIsLoading] = useState(false);

  const formControls = useForm<TeslaActivationType>({
    resolver: zodResolver(teslaActivationSchema),
    defaultValues: {
      vin: '',
      email: user?.email || '',
    },
  });

  // Update form when user data is available
  useEffect(() => {
    if (user) {
      formControls.setValue('email', user.email);
    }
  }, [user, formControls]);

  const onSubmit = async (data: TeslaActivationType) => {
    setIsLoading(true);
    try {
      const response = await fetchWithAuth<string>(ROUTES.TESLA_CREATE_USER, {
        method: 'POST',
        body: JSON.stringify({
          vin: data.vin.trim(),
          email: data.email,
        }),
      });

      if (!response) {
        toast.error('Error creating Tesla user');
      } else {
        // Redirect to Tesla authentication URL
        window.location.href = response;
      }
    } catch (error) {
      toast.error('Error creating Tesla user: ' + (error as Error).message);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <FormProvider {...formControls}>
      <form onSubmit={formControls.handleSubmit(onSubmit)} name={'teslaActivationForm'}>
        <div className="flex flex-col gap-8 w-1/2 mx-auto">
          <div className="flex flex-col w-full gap-2">
            <p className="text-gray">Provide your VIN</p>
          </div>

          <StringFormInput
            name="vin"
            label="VIN"
            placeholder="Enter your VIN"
            required
            className=""
          />

          <StringFormInput
            name="email"
            label="Email"
            placeholder="Enter your email"
            type="email"
            required
            className=""
          />
          <div className="flex w-full justify-end">
            <Button type="submit" className="px-6" loading={isLoading}>
              Next <IconArrowRight size={17} />
            </Button>
          </div>
        </div>
      </form>
    </FormProvider>
  );
};
