import * as z from 'zod';


export const teslaActivationSchema = z.object({
  vin: z.string().min(17, { message: 'VIN must be 17 characters long' }),
  email: z.string().email({ message: 'Invalid email address' })
});

export type TeslaActivationForm = z.infer<typeof teslaActivationSchema>;
