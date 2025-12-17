import React from 'react';
import { View, Text, Link } from '@react-pdf/renderer';
import { LanguageEnum } from '@/components/entities/flash-report/forms/schema';
import { pdfTexts } from '@/components/pdf/pdf_texts';

const sectionStyle = {
  backgroundColor: '#fff',
  borderRadius: 12,
  paddingLeft: 12,
  paddingRight: 12,
  paddingTop: 6,
  paddingBottom: 6,
  marginBottom: 4,
  border: '1px solid #E5E7EB',
};

const titleStyle = {
  fontSize: 12,
  fontWeight: 'medium',
  marginBottom: 4,
  color: '#111827',
};

const subtitleStyle = {
  fontSize: 9,
  fontWeight: 'medium',
  marginBottom: 2,
  color: '#111827',
};

const textStyle = {
  fontSize: 8,
  color: '#374151',
  marginBottom: 2,
};

const PdfDetailedInformation: React.FC<{
  warranty_date: number;
  warranty_km: number;
}> = ({ warranty_date, warranty_km }) => {
  const texts = pdfTexts[LanguageEnum.EN];

  return (
    <View>
      {/* State of Health (SoH) */}
      <View style={{ ...sectionStyle }}>
        <Text style={titleStyle}>{texts.soh_information.title}</Text>
        <Text style={textStyle}>{texts.soh_information.explanation}</Text>
        <Text style={textStyle}>{texts.soh_information.explanation_2}</Text>
      </View>
      {/* Bib Score */}
      <View style={{ ...sectionStyle }}>
        <Text style={titleStyle}>Bib Score</Text>
        <View style={{ flexDirection: 'row', alignItems: 'baseline' }}>
          <Text style={textStyle}>The</Text>
          <Text style={subtitleStyle}> Bib Score</Text>
          <Text style={textStyle}> is based on several factors:</Text>
        </View>
        <Text style={textStyle}>
          • SoH, which reflects the battery's condition compared to its original state.
        </Text>
        <Text style={textStyle}>
          • A comparison of this vehicle's aging against the average aging of similar
          vehicles.
        </Text>
        <Text style={textStyle}>
          For example, a vehicle with a "good" SoH (e.g., 85%) may still receive a "poor"
          Bib Score (e.g., D) if it falls significantly below the average for similar
          vehicles.
        </Text>
      </View>
      {/* Vehicle & Battery Warranty */}
      <View style={sectionStyle}>
        <Text style={titleStyle}>{texts.vehicle_and_battery_warranty.title}</Text>
        <Text style={textStyle}>{texts.vehicle_and_battery_warranty.explanation}</Text>
        <Text style={textStyle}>{texts.vehicle_and_battery_warranty.explanation_2}</Text>
        <Text style={textStyle}>
          {texts.vehicle_and_battery_warranty.explanation_3(warranty_date, warranty_km)}
        </Text>
      </View>
      {/* Range Insights */}
      <View style={sectionStyle}>
        <Text style={titleStyle}>{texts.range_insights.title}</Text>
        <Text style={textStyle}>{texts.range_insights.explanation}</Text>
      </View>
      {/* Report Validity */}
      <View style={sectionStyle}>
        <Text style={titleStyle}>{texts.report_validity.title}</Text>
        <Text style={textStyle}>
          {texts.report_validity.explanation}{' '}
          <Link
            src="https://www.bib-batteries.fr"
            style={{ color: '#2563eb', textDecoration: 'underline' }}
          >
            www.bib-batteries.fr
          </Link>
          .
        </Text>
      </View>
    </View>
  );
};

export default PdfDetailedInformation;
