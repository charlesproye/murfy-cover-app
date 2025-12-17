'use client';

import React from 'react';
import { Document, Page, Font, StyleSheet, View } from '@react-pdf/renderer';
import PdfHeaderFlash from '@/components/pdf/flash-report/PdfHeaderFlash';
import PdfSohChartFlash from '@/components/pdf/flash-report/PdfSohChartFlash';
import PdfChargingSection from '@/components/pdf/PdfChargingSection';
import PdfFooter from '@/components/pdf/PdfFooter';
import { GetGenerationInfo } from '@/interfaces/flash-reports';
import PdfRangeAndWarranty from '@/components/pdf/PdfRangeAndWarranty';
import PdfDetailedInformationFlash from '@/components/pdf/flash-report/PdfDetailedInformationFlash';

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

export const FlashReportPdf: React.FC<{ data: GetGenerationInfo }> = ({ data }) => {
  return (
    <Document>
      <Page size="A4" style={styles.page}>
        <View style={styles.headerContent}>
          <PdfHeaderFlash data={data} />
        </View>

        <View style={styles.content}>
          <PdfSohChartFlash data={data} />
        </View>
        <View style={styles.content}>
          <PdfRangeAndWarranty
            warranty_km={data.vehicle_info.warranty_km}
            warranty_date={data.vehicle_info.warranty_date}
            mileage={data.vehicle_info.mileage}
            range={data.battery_info.range}
            language={data.language}
            trendline={data.battery_info.trendline ?? null}
          />
        </View>
        <View style={styles.footerContent}>
          <PdfFooter language={data.language} />
        </View>
      </Page>
      <Page size="A4" style={styles.page}>
        <View style={styles.content}>
          <PdfChargingSection
            brand={data.vehicle_info.brand}
            model={data.vehicle_info.model}
            batteryCapacity={data.battery_info?.capacity}
            language={data.language}
          />
        </View>
        <View style={styles.content}>
          <PdfDetailedInformationFlash
            language={data.language}
            warranty_date={data.vehicle_info.warranty_date}
            warranty_km={data.vehicle_info.warranty_km}
          />
        </View>
        <View style={styles.footerContent}>
          <PdfFooter language={data.language} />
        </View>
      </Page>
    </Document>
  );
};
