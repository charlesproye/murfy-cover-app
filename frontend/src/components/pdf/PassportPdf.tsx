'use client';

import React from 'react';
import { Document, Page, Font, StyleSheet, View } from '@react-pdf/renderer';
import PdfHeader from '@/components/pdf/PdfHeader';
import PdfSohChart from '@/components/pdf/PdfSohChart';
import PdfChargingSection from '@/components/pdf/PdfChargingSection';
import PdfFooter from '@/components/pdf/PdfFooter';
import { InfoVehicleResult } from '@/interfaces/dashboard/passport/infoVehicle';
import PdfRangeAndWarranty from '@/components/pdf/PdfRangeAndWarranty';
import PdfDetailedInformation from '@/components/pdf/PdfDetailedInformation';

Font.register({
  family: 'Poppins',
  fonts: [
    { src: '/font/Poppins/Poppins-Regular.ttf' },
    { src: '/font/Poppins/Poppins-Bold.ttf', fontWeight: 'bold' },
    { src: '/font/Poppins/Poppins-SemiBold.ttf', fontWeight: 'semibold' },
    { src: '/font/Poppins/Poppins-Medium.ttf', fontWeight: 'medium' },
    { src: '/font/Poppins/Poppins-Light.ttf', fontWeight: 'light' },
  ],
});

const styles = StyleSheet.create({
  page: {
    padding: 4,
    fontFamily: 'Poppins',
    backgroundColor: '#F6F7F9',
  },
  content: {
    marginTop: 4,
    marginBottom: 4,
  },
  footerContent: {
    position: 'absolute',
    bottom: 0,
    left: 0,
    right: 0,
    margin: 4,
  },
  headerContent: {
    marginTop: 4,
    marginBottom: 4,
  },
});

const PassportPdf: React.FC<{ data: InfoVehicleResult }> = ({ data }) => {
  return (
    <Document>
      <Page size="A4" style={styles.page}>
        <View style={styles.headerContent}>
          <PdfHeader data={data} />
        </View>

        <View style={styles.content}>
          <PdfSohChart data={data} />
        </View>
        <View style={styles.content}>
          <PdfRangeAndWarranty
            warranty_km={data.vehicle_info.warranty_km}
            warranty_date={data.vehicle_info.warranty_date}
            mileage={data.vehicle_info.odometer}
            start_date={data.vehicle_info.start_date}
            range={data.battery_info.range}
            trendline={data.battery_info.trendline}
          />
        </View>
        <View style={styles.footerContent}>
          <PdfFooter />
        </View>
      </Page>
      <Page size="A4" style={styles.page}>
        <View style={styles.content}>
          <PdfChargingSection
            brand={data.vehicle_info.brand}
            model={data.vehicle_info.model}
            batteryCapacity={data.battery_info?.capacity}
          />
        </View>
        <View style={styles.content}>
          <PdfDetailedInformation
            warranty_date={data.vehicle_info.warranty_date}
            warranty_km={data.vehicle_info.warranty_km}
          />
        </View>
        <View style={styles.footerContent}>
          <PdfFooter />
        </View>
      </Page>
    </Document>
  );
};

export default PassportPdf;

export { styles };
