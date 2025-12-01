'use client';

import React, { useEffect, useMemo, useState } from 'react';
import { FormProvider, useForm, useWatch } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { useRouter } from 'next/navigation';
import { Button } from '@/components/ui/button';
import { IconArrowLeft, IconArrowRight, IconLoader2 } from '@tabler/icons-react';
import { StringFormInput } from '@/components/form/StringFormInput';
import { NumberFormInput } from '@/components/form/NumberFormInput';
import useSWR from 'swr';
import { ROUTES } from '@/routes';
import { fetchWithoutAuth } from '@/services/fetchWithoutAuth';
import { SelectFormInput } from '@/components/form/SelectFormInput';
import { toast } from 'sonner';
import { useBrowserLanguage } from '@/hooks/utils/useBrowserLanguage';
import { capitalizeFirstLetter } from '@/lib/utils';
import {
  flashReportFormSchema,
  FlashReportFormType,
  LanguageEnum,
} from '@/components/flash-report/forms/schema';
import type { VinDecoderTypeVersion } from '@/components/flash-report/forms/VinForm';

interface FlashReportFormProps {
  vin: string;
  has_trendline: boolean;
  make: string | undefined;
  model: string | undefined;
  type_version_list: VinDecoderTypeVersion[] | null;
}

interface AllMakesModelsInfo {
  makes: Array<{
    make_name: string;
    models: Array<{
      model_name: string;
      types: Array<{
        model_type: string;
        versions: Array<string>;
      }>;
    }>;
  }>;
}

export const FlashReportForm = ({
  vin,
  has_trendline,
  make,
  model,
  type_version_list,
}: FlashReportFormProps): React.ReactElement => {
  const router = useRouter();
  const [isLoadingSendEmail, setIsLoadingSendEmail] = useState(false);
  const [isFinalMessage, setIsFinalMessage] = useState(false);
  const browserLanguage = useBrowserLanguage();
  const { data: allVehicleModels, isLoading: isLoadingAllVehicleModels } = useSWR(
    ROUTES.ALL_MODELS_WITH_TRENDLINE,
    fetchWithoutAuth<AllMakesModelsInfo>,
  );

  const defaultValues = {
    vin: vin,
    has_trendline: has_trendline,
    make: make ?? '',
    model: model ?? '',
    type:
      type_version_list && type_version_list.length > 0 ? type_version_list[0].type : '',
    odometer: 0,
    email: '',
    language: browserLanguage.toUpperCase() as LanguageEnum,
  };

  const formControls = useForm({
    resolver: zodResolver(flashReportFormSchema),
    defaultValues,
  });
  const makeValue = useWatch({ control: formControls.control, name: 'make' });
  const modelValue = useWatch({ control: formControls.control, name: 'model' });
  const languageValue = useWatch({ control: formControls.control, name: 'language' });

  useEffect(() => {
    if (browserLanguage) {
      formControls.setValue(
        'language',
        browserLanguage === LanguageEnum.FR ? LanguageEnum.FR : LanguageEnum.EN,
      );
    }
  }, [browserLanguage]);

  const makeOptions = useMemo(() => {
    return (
      allVehicleModels?.makes.map((make) => ({
        value: make.make_name,
        label: capitalizeFirstLetter(make.make_name),
      })) ?? []
    );
  }, [allVehicleModels]);

  const modelOptions = useMemo(() => {
    if (makeValue === '') return [];
    const models = allVehicleModels?.makes.find(
      (make) => make.make_name === makeValue,
    )?.models;
    return (
      models?.map((model) => ({
        value: model.model_name,
        label: capitalizeFirstLetter(model.model_name),
      })) ?? []
    );
  }, [allVehicleModels, makeValue]);

  const typeOptions = useMemo(() => {
    if (makeValue === '' || modelValue === '') return [];
    const models = allVehicleModels?.makes.find(
      (make) => make.make_name === makeValue,
    )?.models;
    const types = models?.find((model) => model.model_name === modelValue)?.types;
    return (
      types?.map((type) => ({
        value: type.model_type,
        label: capitalizeFirstLetter(type.model_type),
      })) ?? []
    );
  }, [allVehicleModels, makeValue, modelValue]);

  const onSubmit = async (data: FlashReportFormType) => {
    setIsLoadingSendEmail(true);
    try {
      const type = data.type || (typeOptions.length > 0 ? typeOptions[0].value : '');
      const version =
        data.make === 'tesla' &&
        data.model === model &&
        type_version_list &&
        type_version_list.length > 0
          ? type_version_list.find((t) => t.type === type)?.version
          : undefined;

      const response = await fetchWithoutAuth<{ message: string }>(ROUTES.SEND_EMAIL, {
        method: 'POST',
        body: JSON.stringify({
          ...data,
          type,
          ...(version ? { version } : {}),
        }),
      });
      if (!response) {
        toast.error('Error sending report email');
      } else {
        // Clear localStorage after successful submission
        localStorage.removeItem('flash-report-data');
        setIsFinalMessage(true);
      }
    } catch (error) {
      toast.error('Error sending report email: ' + (error as Error).message);
    } finally {
      setIsLoadingSendEmail(false);
    }
  };

  if (isFinalMessage) {
    return (
      <div className="flex flex-col gap-8">
        <p className="text-xl font-medium">Your report was successfully sent by email.</p>
        <Button
          type="button"
          className="px-6 max-w-48"
          onClick={() => router.push('/flash-report')}
        >
          Get another report <IconArrowRight size={17} />
        </Button>
      </div>
    );
  }

  return (
    <FormProvider {...formControls}>
      <form onSubmit={formControls.handleSubmit(onSubmit)} name={'flashReportForm'}>
        <div className="flex flex-col gap-8">
          <div className="flex w-full gap-2 flex-col">
            <p className="text-xl font-medium">Validate vehicle information</p>
            {isLoadingAllVehicleModels && (
              <IconLoader2 className="w-6 h-6 animate-spin" />
            )}

            {has_trendline ? (
              <p className="text-sm text-gray-500 italic">
                Please confirm and specify the vehicle information below.
              </p>
            ) : (
              <p className="text-sm text-warning/60 italic">
                The current VIN does not seems to be from a vehicle referenced in our
                database. Please select it manually among our available models listed
                below.
              </p>
            )}
          </div>

          {!isLoadingAllVehicleModels && !allVehicleModels ? (
            <div className="flex flex-col gap-4 border border-gray-200 rounded-lg p-4 items-center">
              <p className="text-sm text-gray-500">
                No vehicles data found. Please try again or contact us if the problems
                persists.
              </p>
              <Button
                type="button"
                className="px-6 max-w-48"
                onClick={() => router.push('/flash-report')}
              >
                Back <IconArrowLeft size={17} />
              </Button>
            </div>
          ) : (
            <>
              <div className="flex flex-col gap-4 border border-gray-200 rounded-lg p-4">
                <div className="flex justify-center gap-4">
                  <SelectFormInput
                    name="make"
                    label="Make"
                    placeholder="Select your make"
                    options={makeOptions}
                    onChange2={() => {
                      formControls.setValue('model', '');
                      formControls.setValue('type', '');
                    }}
                    required
                    className="w-1/2"
                  />
                  <SelectFormInput
                    name="model"
                    label="Model"
                    placeholder="Select your model"
                    options={modelOptions}
                    onChange2={() => {
                      formControls.setValue('type', '');
                    }}
                    required
                    className="w-1/2"
                  />
                </div>
                {typeOptions.length > 1 && (
                  <SelectFormInput
                    name="type"
                    label="Type"
                    placeholder="Enter your type"
                    options={typeOptions}
                    className="w-1/2"
                  />
                )}

                <div className="flex justify-between gap-4">
                  <NumberFormInput
                    name="odometer"
                    label="Mileage (km)"
                    placeholder="Enter your odometer"
                    required
                    className="w-1/3"
                  />
                  <SelectFormInput
                    name="language"
                    label="Language"
                    options={[
                      { value: LanguageEnum.EN, label: 'English' },
                      { value: LanguageEnum.FR, label: 'French' },
                    ]}
                    required
                    className="w-1/2"
                  />
                </div>
                <StringFormInput
                  name="email"
                  label="Email"
                  placeholder="Enter your email"
                  required
                  className="max-w-96"
                />
              </div>

              <div className="flex flex-col gap-2">
                <p className="text-xs text-gray-600 leading-relaxed">
                  {languageValue === LanguageEnum.FR ? (
                    <>
                      <strong>Protection des données personnelles :</strong> Les
                      informations collectées (VIN, données du véhicule et adresse e-mail)
                      sont utilisées uniquement pour générer et vous envoyer votre rapport
                      d'état de santé de batterie. Ces données ne sont pas conservées de
                      manière permanente et ne sont utilisées à aucune autre fin.
                      Conformément au RGPD, vous disposez d'un droit d'accès, de
                      rectification et de suppression de vos données. Pour exercer ces
                      droits, contactez-nous à{' '}
                      <a
                        href="mailto:support@bib-batteries.fr"
                        className="text-blue-600 hover:underline"
                      >
                        support@bib-batteries.fr
                      </a>
                      . Aucune donnée supplémentaire n'est collectée sur votre véhicule.
                    </>
                  ) : (
                    <>
                      <strong>Personal Data Protection:</strong> The information collected
                      (VIN, vehicle data, and email address) is used solely to generate
                      and send you your battery state-of-health report. This data is not
                      permanently stored and is not used for any other purpose. In
                      accordance with GDPR, you have the right to access, rectify, and
                      delete your data. To exercise these rights, please contact us at{' '}
                      <a
                        href="mailto:support@bib-batteries.fr"
                        className="text-blue-600 hover:underline"
                      >
                        support@bib-batteries.fr
                      </a>
                      . No additional data is collected on your vehicle.
                    </>
                  )}
                </p>
              </div>

              <div className="flex w-full justify-end">
                <Button type="submit" className="px-6 mr-6" loading={isLoadingSendEmail}>
                  Submit <IconArrowRight size={17} />
                </Button>
              </div>
            </>
          )}
        </div>
      </form>
    </FormProvider>
  );
};
