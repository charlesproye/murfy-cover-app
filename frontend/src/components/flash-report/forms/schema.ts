import * as z from 'zod';

export enum LanguageEnum {
  EN = 'EN',
  FR = 'FR',
}

export const vinDecoderSchema = z.object({
  vin: z.string().trim().length(17, { message: 'VIN must be 17 characters long' }),
});

export const flashReportFormSchema = z.object({
  vin: z.string(),
  make: z.string().min(1, { message: 'Make is required' }),
  model: z.string().min(1, { message: 'Model is required' }),
  type: z.string().optional(),
  odometer: z.coerce.number().min(1, { message: 'Odometer cannot be zero' }),
  email: z.string().email(),
  language: z.enum([LanguageEnum.EN, LanguageEnum.FR]).default(LanguageEnum.EN),
});

export type VinDecoder = z.infer<typeof vinDecoderSchema>;
export type FlashReportFormType = z.infer<typeof flashReportFormSchema>;
