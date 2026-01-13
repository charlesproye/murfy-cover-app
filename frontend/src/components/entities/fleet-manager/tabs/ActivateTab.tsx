'use client';

import React, { useState, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { IconCheck, IconUpload, IconDownload } from '@tabler/icons-react';
import { toast } from 'sonner';
import fetchWithAuth from '@/services/fetchWithAuth';
import { ROUTES } from '@/routes';
import {
  FleetInfo,
  ActivationRequest,
  ActivationResponse,
  VehicleStatus,
  MakeInfo,
  MakeInfoResponse,
  ModelInfo,
  ModelInfoResponse,
} from '@/interfaces/fleet-manager/fleet-manager';

interface ActivateTabProps {
  fleets: FleetInfo[];
  selectedFleetId: string | null;
  setSelectedFleetId: (fleetId: string | null) => void;
}

type InputMethod = 'single' | 'csv';

interface CSVValidationRow {
  originalRow: any;
  index: number;
  vin: string;
  make: string;
  model: string;
  type: string;
  startDate: string;
  endDate: string;
  startDateValid: boolean;
  endDateValid: boolean;
  makeValid: boolean;
  makeId?: string;
  suggestedModel?: ModelInfo;
  suggestedType?: string;
  availableModels?: ModelInfo[]; // All models available for the make
  availableTypes?: string[]; // All types available for the model
  similarityScore?: number;
  status: 'valid' | 'suggestion' | 'error';
  errorMessage?: string;
}

const ActivateTab: React.FC<ActivateTabProps> = ({
  fleets,
  selectedFleetId,
  setSelectedFleetId
}) => {
  const [inputMethod, setInputMethod] = useState<InputMethod>('single');
  const [isLoading, setIsLoading] = useState(false);
  const [results, setResults] = useState<VehicleStatus[] | null>(null);

  // Available makes and models
  const [availableMakes, setAvailableMakes] = useState<MakeInfo[]>([]);
  const [availableModels, setAvailableModels] = useState<ModelInfo[]>([]);
  const [isLoadingMakes, setIsLoadingMakes] = useState(false);
  const [isLoadingModels, setIsLoadingModels] = useState(false);

  // Single vehicle form
  const [vin, setVin] = useState('');
  const [selectedMakeId, setSelectedMakeId] = useState('');
  const [selectedModelId, setSelectedModelId] = useState('');
  const [vehicleType, setVehicleType] = useState('');
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [comment, setComment] = useState('');

  // CSV import
  const [csvFile, setCsvFile] = useState<File | null>(null);
  const [csvPreview, setCsvPreview] = useState<any[]>([]);
  const [csvValidation, setCsvValidation] = useState<CSVValidationRow[]>([]);
  const [isValidatingCSV, setIsValidatingCSV] = useState(false);
  const [showValidation, setShowValidation] = useState(false);

  // Load available makes on mount
  useEffect(() => {
    const loadMakes = async () => {
      setIsLoadingMakes(true);
      try {
        const response = await fetchWithAuth<MakeInfoResponse>(
          ROUTES.VEHICLE_MAKES
        );
        if (response && response.makes) {
          setAvailableMakes(response.makes);
        }
      } catch (error) {
        toast.error('Failed to load available makes');
        console.error(error);
      } finally {
        setIsLoadingMakes(false);
      }
    };

    loadMakes();

    // Load persisted CSV data from localStorage
    const savedCSV = localStorage.getItem('activateTab_csvValidation');
    if (savedCSV) {
      try {
        const parsed = JSON.parse(savedCSV);
        setCsvValidation(parsed);
        setShowValidation(true);
        if (parsed.length > 0) {
          setInputMethod('csv');
        }
      } catch (error) {
        console.error('Failed to parse saved CSV data:', error);
      }
    }
  }, []);

  // Save CSV validation to localStorage whenever it changes
  useEffect(() => {
    if (csvValidation.length > 0) {
      localStorage.setItem('activateTab_csvValidation', JSON.stringify(csvValidation));
    }
  }, [csvValidation]);

  // Load models when make is selected
  useEffect(() => {
    if (!selectedMakeId) {
      setAvailableModels([]);
      setSelectedModelId('');
      setVehicleType('');
      return;
    }

    const loadModels = async () => {
      setIsLoadingModels(true);
      try {
        const response = await fetchWithAuth<ModelInfoResponse>(
          ROUTES.VEHICLE_MODELS(selectedMakeId)
        );
        if (response && response.models) {
          setAvailableModels(response.models);
        }
      } catch (error) {
        toast.error('Failed to load models for this make');
        console.error(error);
      } finally {
        setIsLoadingModels(false);
      }
    };

    loadModels();
  }, [selectedMakeId]);

  // Update type when model is selected
  useEffect(() => {
    if (selectedModelId) {
      // Get the selected model to check available types
      const selectedModel = availableModels.find(m => m.model_id === selectedModelId);
      // Reset type when changing model
      setVehicleType('');
    }
  }, [selectedModelId, availableModels]);

  const resetForm = () => {
    setVin('');
    setSelectedMakeId('');
    setSelectedModelId('');
    setVehicleType('');
    setStartDate('');
    setEndDate('');
    setComment('');
    setCsvFile(null);
    setCsvPreview([]);
    setCsvValidation([]);
    setShowValidation(false);
    setResults(null);
    // Remove from localStorage
    localStorage.removeItem('activateTab_csvValidation');
  };

  const clearCSVData = () => {
    setCsvFile(null);
    setCsvPreview([]);
    setCsvValidation([]);
    setShowValidation(false);
    localStorage.removeItem('activateTab_csvValidation');
    toast.success('CSV data cleared');
  };

  // Calculate similarity between two strings (0-1, 1 = identical)
  const calculateSimilarity = (str1: string, str2: string): number => {
    const s1 = str1.toLowerCase().trim();
    const s2 = str2.toLowerCase().trim();

    if (s1 === s2) return 1;

    // Simple similarity: check if one contains the other
    if (s1.includes(s2) || s2.includes(s1)) {
      return 0.8;
    }

    // Levenshtein distance
    const matrix: number[][] = [];
    for (let i = 0; i <= s1.length; i++) {
      matrix[i] = [i];
    }
    for (let j = 0; j <= s2.length; j++) {
      matrix[0][j] = j;
    }

    for (let i = 1; i <= s1.length; i++) {
      for (let j = 1; j <= s2.length; j++) {
        if (s1[i - 1] === s2[j - 1]) {
          matrix[i][j] = matrix[i - 1][j - 1];
        } else {
          matrix[i][j] = Math.min(
            matrix[i - 1][j - 1] + 1,
            matrix[i][j - 1] + 1,
            matrix[i - 1][j] + 1
          );
        }
      }
    }

    const distance = matrix[s1.length][s2.length];
    const maxLength = Math.max(s1.length, s2.length);
    return 1 - distance / maxLength;
  };

  const validateAndConvertDate = (dateStr: string): { isValid: boolean; isoDate: string } => {
    if (!dateStr || !dateStr.trim()) {
      return { isValid: true, isoDate: '' };
    }

    const isoRegex = /^\d{4}-\d{2}-\d{2}$/;
    if (isoRegex.test(dateStr)) {
      const date = new Date(dateStr);
      if (!isNaN(date.getTime())) {
        return { isValid: true, isoDate: dateStr };
      }
    }

    const formats = [
      /^(\d{2})\/(\d{2})\/(\d{4})$/,
      /^(\d{2})-(\d{2})-(\d{4})$/,
      /^(\d{4})\/(\d{2})\/(\d{2})$/,
    ];

    for (const format of formats) {
      const match = dateStr.match(format);
      if (match) {
        let year, month, day;

        if (format === formats[0] || format === formats[1]) {
          [, day, month, year] = match;
        } else {
          [, year, month, day] = match;
        }

        const date = new Date(`${year}-${month}-${day}`);
        if (!isNaN(date.getTime())) {
          return { isValid: true, isoDate: `${year}-${month}-${day}` };
        }
      }
    }

    return { isValid: false, isoDate: dateStr };
  };

  // Validate and suggest corrections for CSV data
  const validateCSVData = async (data: any[]) => {
    setIsValidatingCSV(true);
    const validatedRows: CSVValidationRow[] = [];

    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      const validationRow: CSVValidationRow = {
        originalRow: row,
        index: i,
        vin: row.vin,
        make: row.make,
        model: row.model,
        type: row.type || '',
        startDate: row.start_date || '',
        endDate: row.end_date || '',
        startDateValid: false,
        endDateValid: false,
        makeValid: false,
        status: 'error',
      };

      // Check if make exists
      const makeMatch = availableMakes.find(
        m => m.make_name.toLowerCase() === row.make.toLowerCase().trim()
      );

      if (makeMatch) {
        validationRow.makeValid = true;
        validationRow.makeId = makeMatch.make_id;

        // If make doesn't have conditions (OEMS_W_SOH_W_MODEL_API), model and type are optional
        // Can be valid with just make
        if (!makeMatch.make_conditions) {
          // Model and type are optional for this make
          if (!row.model || !row.model.trim()) {
            // No model provided, mark as valid with just the make
            validationRow.status = 'valid';
            validationRow.model = '';
            validationRow.type = '';
          } else {
            // Model provided, try to find it
            try {
              const response = await fetchWithAuth<ModelInfoResponse>(
                ROUTES.VEHICLE_MODELS(makeMatch.make_id)
              );

              if (response && response.models) {
                validationRow.availableModels = response.models;

                // Find best matching model
                let bestMatch: ModelInfo | null = null;
                let bestScore = 0;

                response.models.forEach(model => {
                  const score = calculateSimilarity(row.model, model.model_name);
                  if (score > bestScore) {
                    bestScore = score;
                    bestMatch = model;
                  }
                });

                if (bestMatch && bestScore > 0.6) {
                  const matchedModel = bestMatch as ModelInfo;
                  validationRow.suggestedModel = matchedModel;
                  validationRow.similarityScore = bestScore;
                  validationRow.model = matchedModel.model_name;

                  const typesForThisModel = Array.from(
                    new Set(
                      response.models
                        .filter(m => m.model_name === matchedModel.model_name && m.type)
                        .map(m => m.type as string)
                    )
                  );
                  validationRow.availableTypes = typesForThisModel;

                  if (matchedModel.type) {
                    validationRow.suggestedType = matchedModel.type;
                  }

                  if (row.type && typesForThisModel.some(t => t.toLowerCase() === row.type.toLowerCase())) {
                    const matchedType = typesForThisModel.find(t => t.toLowerCase() === row.type.toLowerCase());
                    validationRow.type = matchedType || row.type;
                    validationRow.status = bestScore === 1 ? 'valid' : 'suggestion';
                  } else {
                    validationRow.type = '';
                    validationRow.status = 'suggestion';
                  }
                } else {
                  // No good model match, but since model is optional, mark as valid
                  validationRow.status = 'valid';
                  validationRow.model = row.model;
                  validationRow.type = row.type || '';
                }
              }
            } catch (error) {
              console.error('Error loading models:', error);
              // Even with error, mark as valid since model is optional
              validationRow.status = 'valid';
              validationRow.model = row.model;
              validationRow.type = row.type || '';
            }
          }
        } else {
          // Model and type are REQUIRED for this make (OEMS_W_SOH_WO_MODEL_API)

          // Check if model or type is missing
          if (!row.model || !row.model.trim()) {
            validationRow.status = 'error';
            validationRow.errorMessage = `Model is required for ${row.make}`;
          } else {
            // Load models for this make
            try {
              const response = await fetchWithAuth<ModelInfoResponse>(
                ROUTES.VEHICLE_MODELS(makeMatch.make_id)
              );

              if (response && response.models) {
                // Store all available models for this make
                validationRow.availableModels = response.models;

                // Find best matching model
                let bestMatch: ModelInfo | null = null;
                let bestScore = 0;

                response.models.forEach(model => {
                  const score = calculateSimilarity(row.model, model.model_name);
                  if (score > bestScore) {
                    bestScore = score;
                    bestMatch = model;
                  }
                });

                if (bestMatch && bestScore > 0.6) {
                  const matchedModel = bestMatch as ModelInfo;
                  validationRow.suggestedModel = matchedModel;
                  validationRow.similarityScore = bestScore;
                  validationRow.model = matchedModel.model_name;

                  // Get all types available ONLY for this specific model name
                  const typesForThisModel = Array.from(
                    new Set(
                      response.models
                        .filter(m => m.model_name === matchedModel.model_name && m.type)
                        .map(m => m.type as string)
                    )
                  );
                  validationRow.availableTypes = typesForThisModel;

                  // Store suggested type but don't auto-fill
                  if (matchedModel.type) {
                    validationRow.suggestedType = matchedModel.type;
                  }

                  // Check if the original type from CSV matches one of the available types
                  if (row.type && typesForThisModel.some(t => t.toLowerCase() === row.type.toLowerCase())) {
                    const matchedType = typesForThisModel.find(t => t.toLowerCase() === row.type.toLowerCase());
                    validationRow.type = matchedType || row.type;
                    if (bestScore === 1) {
                      validationRow.status = 'valid';
                    } else {
                      validationRow.status = 'suggestion';
                    }
                  } else {
                    // Type doesn't match or is empty, user needs to select
                    // For makes with conditions, type is REQUIRED
                    validationRow.type = '';
                    if (!row.type || !row.type.trim()) {
                      validationRow.status = 'suggestion';
                      validationRow.errorMessage = `Type is required for ${row.make}`;
                    } else {
                      validationRow.status = 'suggestion';
                    }
                  }
                } else {
                  validationRow.status = 'error';
                  validationRow.errorMessage = `No matching model found for "${row.model}"`;
                }
              }
            } catch (error) {
              console.error('Error loading models:', error);
              validationRow.status = 'error';
              validationRow.errorMessage = 'Failed to load models';
            }
          }
        }
      } else {
        validationRow.status = 'error';
        validationRow.errorMessage = `Make "${row.make}" not found in available makes`;
      }
      const startDateResult = validateAndConvertDate(row.start_date || '');
      const endDateResult = validateAndConvertDate(row.end_date || '');

      validationRow.startDate = startDateResult.isoDate;
      validationRow.endDate = endDateResult.isoDate;
      validationRow.startDateValid = startDateResult.isValid;
      validationRow.endDateValid = endDateResult.isValid;

      // Si les dates sont invalides, marquer comme erreur
      if (!startDateResult.isValid || !endDateResult.isValid) {
        validationRow.status = 'error';
        validationRow.errorMessage = `Invalid date format: ${
          !startDateResult.isValid ? 'start_date' : 'end_date'
        }`;
      }
      validatedRows.push(validationRow);
    }

    setCsvValidation(validatedRows);
    setShowValidation(true);
    setIsValidatingCSV(false);
  };

  const acceptSuggestion = (index: number) => {
    const updated = [...csvValidation];
    const row = updated[index];
    if (row.suggestedModel) {
      row.model = row.suggestedModel.model_name;

      // If there's a suggested type, use it
      if (row.suggestedType) {
        row.type = row.suggestedType;
        row.status = 'valid';
        row.similarityScore = 1;
      } else if (row.availableTypes && row.availableTypes.length > 0) {
        // If no suggested type but types are available, keep as suggestion for user to select
        row.status = 'suggestion';
      } else {
        // No types available, mark as valid anyway
        row.status = 'valid';
        row.similarityScore = 1;
      }
    }
    setCsvValidation(updated);
  };

  const rejectSuggestion = (index: number) => {
    const updated = [...csvValidation];
    updated[index].status = 'error';
    updated[index].errorMessage = 'Suggestion rejected by user';
    setCsvValidation(updated);
  };

  const updateType = (index: number, newType: string) => {
    const updated = [...csvValidation];
    updated[index].type = newType;
    // If type is selected and model is accepted, mark as valid
    if (updated[index].suggestedModel && newType) {
      updated[index].status = 'valid';
    }
    setCsvValidation(updated);
  };

  const updateModel = (index: number, modelId: string) => {
    const updated = [...csvValidation];
    const row = updated[index];

    // Find the selected model
    const selectedModel = row.availableModels?.find(m => m.model_id === modelId);

    if (selectedModel) {
      row.model = selectedModel.model_name;
      row.suggestedModel = selectedModel;

      // Update available types for this new model
      const typesForThisModel = Array.from(
        new Set(
          row.availableModels
            ?.filter(m => m.model_name === selectedModel.model_name && m.type)
            .map(m => m.type as string) || []
        )
      );
      row.availableTypes = typesForThisModel;

      // Store suggested type but don't auto-fill
      if (selectedModel.type) {
        row.suggestedType = selectedModel.type;
      } else {
        row.suggestedType = undefined;
      }

      // Reset type and keep as suggestion until user selects a type
      row.type = '';
      row.status = 'suggestion';
    }

    setCsvValidation(updated);
  };

  const updateMake = async (index: number, makeId: string) => {
    const updated = [...csvValidation];
    const row = updated[index];

    // Find the selected make
    const selectedMake = availableMakes.find(m => m.make_id === makeId);

    if (selectedMake) {
      row.make = selectedMake.make_name;
      row.makeValid = true;
      row.makeId = selectedMake.make_id;

      // Load models for this new make
      try {
        const response = await fetchWithAuth<ModelInfoResponse>(
          ROUTES.VEHICLE_MODELS(makeId)
        );

        if (response && response.models) {
          row.availableModels = response.models;

          // Find best matching model
          let bestMatch: ModelInfo | null = null;
          let bestScore = 0;

          response.models.forEach(model => {
            const score = calculateSimilarity(row.originalRow.model, model.model_name);
            if (score > bestScore) {
              bestScore = score;
              bestMatch = model;
            }
          });

          if (bestMatch && bestScore > 0.6) {
            const matchedModel = bestMatch as ModelInfo;
            row.suggestedModel = matchedModel;
            row.similarityScore = bestScore;
            row.model = matchedModel.model_name;

            // Get all types available ONLY for this specific model name
            const typesForThisModel = Array.from(
              new Set(
                response.models
                  .filter(m => m.model_name === matchedModel.model_name && m.type)
                  .map(m => m.type as string)
              )
            );
            row.availableTypes = typesForThisModel;

            // Store suggested type but don't auto-fill
            if (matchedModel.type) {
              row.suggestedType = matchedModel.type;
            }

            // Reset type and keep as suggestion until user selects a type
            row.type = '';
            row.status = 'suggestion';
          } else {
            row.status = 'suggestion';
            row.errorMessage = `No matching model found for "${row.originalRow.model}"`;
          }
        }
      } catch (error) {
        console.error('Error loading models:', error);
        row.status = 'error';
        row.errorMessage = 'Failed to load models';
      }
    }

    setCsvValidation(updated);
  };

  const updateDate = (index: number, field: 'start' | 'end', newDate: string) => {
    const updated = [...csvValidation];
    const result = validateAndConvertDate(newDate);

    if (field === 'start') {
      updated[index].startDate = result.isoDate;
      updated[index].startDateValid = result.isValid;
    } else {
      updated[index].endDate = result.isoDate;
      updated[index].endDateValid = result.isValid;
    }

    // Re-valider le statut de la ligne
    if (!updated[index].startDateValid || !updated[index].endDateValid) {
      updated[index].status = 'error';
      updated[index].errorMessage = `Invalid ${field} date format`;
    } else if (updated[index].status === 'error' && updated[index].errorMessage?.includes('date')) {
      // Si l'erreur était liée à la date, la retirer
      updated[index].status = 'suggestion';
      updated[index].errorMessage = undefined;
    }

    setCsvValidation(updated);
  };

  const downloadCSVTemplate = () => {
    const headers = ['vin', 'make', 'model', 'type', 'start_date', 'end_date', 'comment'];
    const csvContent = headers.join(',');

    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);

    link.setAttribute('href', url);
    link.setAttribute('download', 'vehicle_activation_template.csv');
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);

    toast.success('CSV template downloaded');
  };

  const handleSingleActivation = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!selectedFleetId) {
      toast.error('Please select a fleet');
      return;
    }

    if (vin.length !== 17) {
      toast.error('VIN must be exactly 17 characters');
      return;
    }

    if (!selectedMakeId) {
      toast.error('Make is required');
      return;
    }

    const selectedMake = availableMakes.find(m => m.make_id === selectedMakeId);

    if (!selectedMake) {
      toast.error('Invalid make selection');
      return;
    }

    // For makes WITH conditions (OEMS_W_SOH_WO_MODEL_API), model and type are REQUIRED
    if (selectedMake.make_conditions) {
      if (!selectedModelId) {
        toast.error('Model is required for this make');
        return;
      }
      if (!vehicleType) {
        toast.error('Type is required for this make');
        return;
      }
    }

    // For makes WITHOUT conditions (OEMS_W_SOH_W_MODEL_API - Tesla, BMW, Renault),
    // model and type are optional
    const selectedModel = selectedModelId ? availableModels.find(m => m.model_id === selectedModelId) : null;

    setIsLoading(true);
    try {
      const payload: ActivationRequest = {
        fleet_id: selectedFleetId,
        activation_orders: [
          {
            vehicle: {
              vin: vin.trim(),
              make: selectedMake.make_name,
              model: selectedModel?.model_name || '',
              type: vehicleType || null,
            },
            activation: {
              start_date: startDate || null,
              end_date: endDate || null,
            },
            comment: comment || null,
          },
        ],
      };

      const response = await fetchWithAuth<ActivationResponse>(
        ROUTES.VEHICLE_ACTIVATE,
        {
          method: 'POST',
          body: JSON.stringify(payload),
        }
      );


      if (response && response.vehicles) {
        setResults(response.vehicles);
        toast.success('Activation request sent successfully!');
        // Clear form but keep results
        setVin('');
        setSelectedMakeId('');
        setSelectedModelId('');
        setVehicleType('');
        setStartDate('');
        setEndDate('');
        setComment('');
      }
    } catch (error) {
      console.error('Activation error:', error);
      toast.error('Failed to activate vehicle: ' + (error as Error).message);
    } finally {
      setIsLoading(false);
    }
  };


  const handleCSVUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setCsvFile(file);

    const reader = new FileReader();
    reader.onload = async (event) => {
      const text = event.target?.result as string;
      const lines = text.split('\n').filter(l => l.trim());

      if (lines.length < 2) {
        toast.error('CSV file must have at least a header and one data row');
        return;
      }

      // Detect separator (comma or tab)
      const firstLine = lines[0];
      const separator = firstLine.includes('\t') ? '\t' : ',';

      const headers = firstLine.split(separator).map(h => h.trim().toLowerCase());
      const requiredHeaders = ['vin', 'make', 'model'];

      const missingHeaders = requiredHeaders.filter(h => !headers.includes(h));
      if (missingHeaders.length > 0) {
        toast.error(`Missing required columns: ${missingHeaders.join(', ')}`);
        return;
      }

      const data = lines.slice(1).map(line => {
        const values = line.split(separator).map(v => v.trim().replace(/^"|"$/g, ''));
        const obj: any = {};
        headers.forEach((header, index) => {
          obj[header] = values[index] || '';
        });
        // Convert make to lowercase
        if (obj.make) {
          obj.make = obj.make.toLowerCase();
        }
        return obj;
      });

      setCsvPreview(data);
      toast.success(`${data.length} vehicles found in CSV`);

      // Automatically validate CSV data
      await validateCSVData(data);
    };

    reader.readAsText(file);
  };

  const handleCSVActivation = async () => {
    const selectedFleet = fleets.find(f => f.fleet_id === selectedFleetId);

    if (!selectedFleetId) {
      toast.error('Please select a fleet');
      return;
    }

    if (csvValidation.length === 0) {
      toast.error('Please upload and validate a CSV file');
      return;
    }

    // Filter only valid rows
    const validRows = csvValidation.filter(row => row.status === 'valid');

    if (validRows.length === 0) {
      toast.error('No valid vehicles to activate. Please fix errors and accept suggestions.');
      return;
    }

    // Warn user about skipped rows
    const skippedCount = csvValidation.length - validRows.length;
    if (skippedCount > 0) {
      toast.warning(`${skippedCount} vehicle(s) will be skipped due to errors or pending suggestions.`);
    }

    setIsLoading(true);
    try {
      const payload: ActivationRequest = {
        fleet_id: selectedFleetId,
        activation_orders: validRows.map(row => ({
          vehicle: {
            vin: row.vin,
            make: row.make,
            model: row.suggestedModel?.model_name || row.model,
            type: row.type || null,
          },
          activation: {
            start_date: row.startDate || null,
            end_date: row.endDate || null,
          },
          comment: row.originalRow.comment || null,
        })),
      };

      const response = await fetchWithAuth<ActivationResponse>(
        ROUTES.VEHICLE_ACTIVATE,
        {
          method: 'POST',
          body: JSON.stringify(payload),
        }
      );

      if (response && response.vehicles) {
        setResults(response.vehicles);
        toast.success(`${validRows.length} vehicle(s) processed successfully!`);
        // Clear CSV validation but keep results
        setCsvFile(null);
        setCsvPreview([]);
        setCsvValidation([]);
        setShowValidation(false);
        localStorage.removeItem('activateTab_csvValidation');
      }
    } catch (error) {
      console.error('Activation error:', error);
      toast.error('Failed to activate vehicles: ' + (error as Error).message);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="flex flex-col gap-6 w-full">
      {fleets.length === 0 ? (
        <div className="w-full rounded-lg border bg-white p-12 text-center">
          <p className="text-gray-600">
            Please load your fleets in the &quot;Fleets&quot; tab first
          </p>
        </div>
      ) : (
        <>
          <div className="space-y-2">
            <Label htmlFor="fleet-select-activate">Fleet</Label>
            <Select value={selectedFleetId || ''} onValueChange={setSelectedFleetId}>
              <SelectTrigger id="fleet-select-activate">
                <SelectValue placeholder="Select a fleet" />
              </SelectTrigger>
              <SelectContent>
                {fleets.map((fleet) => (
                  <SelectItem key={fleet.fleet_id} value={fleet.fleet_id}>
                    {fleet.fleet_name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <div className="flex gap-2">
              <button
                onClick={() => { setInputMethod('single'); resetForm(); }}
                className={`px-3 py-1.5 rounded-md text-xs transition-colors ${
                  inputMethod === 'single'
                    ? 'bg-primary text-white'
                    : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                }`}
              >
                Single Vehicle
              </button>
              <button
                onClick={() => { setInputMethod('csv'); resetForm(); }}
                className={`px-3 py-1.5 rounded-md text-xs transition-colors ${
                  inputMethod === 'csv'
                    ? 'bg-primary text-white'
                    : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                }`}
              >
                CSV Import
              </button>
            </div>
          </div>

          {/* Single Vehicle Form */}
          {inputMethod === 'single' && (
            <form onSubmit={handleSingleActivation} className="w-full rounded-lg border bg-white p-6 space-y-4">
              <h3 className="font-semibold text-lg">Vehicle Information</h3>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="vin">VIN *</Label>
                  <Input
                    id="vin"
                    value={vin}
                    onChange={(e) => setVin(e.target.value)}
                    maxLength={17}
                    placeholder="17 characters"
                    required
                  />
                </div>

                <div className="space-y-2">
                  <Label htmlFor="make">Make *</Label>
                  <Select
                    value={selectedMakeId}
                    onValueChange={(value) => {
                      setSelectedMakeId(value);
                      setSelectedModelId('');
                      setVehicleType('');
                    }}
                    disabled={isLoadingMakes}
                  >
                    <SelectTrigger id="make">
                      <SelectValue placeholder={isLoadingMakes ? "Loading makes..." : "Select a make"} />
                    </SelectTrigger>
                    <SelectContent>
                      {availableMakes.map((make) => (
                        <SelectItem key={make.make_id} value={make.make_id}>
                          {make.make_name}
                          {make.make_conditions && (
                            <span className="text-xs text-gray-500 ml-2">
                              ({make.make_conditions})
                            </span>
                          )}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <div className="space-y-2">
                  <Label htmlFor="model">
                    Model {availableMakes.find(m => m.make_id === selectedMakeId)?.make_conditions ? '*' : '(optional)'}
                  </Label>
                  <Select
                    value={selectedModelId}
                    onValueChange={setSelectedModelId}
                    disabled={!selectedMakeId || isLoadingModels}
                  >
                    <SelectTrigger id="model">
                      <SelectValue
                        placeholder={
                          !selectedMakeId
                            ? "Select a make first"
                            : isLoadingModels
                            ? "Loading models..."
                            : "Select a model"
                        }
                      />
                    </SelectTrigger>
                    <SelectContent>
                      {/* Show distinct model names */}
                      {Array.from(
                        new Map(
                          availableModels.map(model => [model.model_name, model])
                        ).values()
                      ).map((model) => (
                        <SelectItem key={model.model_id} value={model.model_id}>
                          {model.model_name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <div className="space-y-2">
                  <Label htmlFor="type">
                    Type {availableMakes.find(m => m.make_id === selectedMakeId)?.make_conditions ? '*' : '(optional)'}
                  </Label>
                  <Select
                    value={vehicleType}
                    onValueChange={setVehicleType}
                    disabled={!selectedModelId}
                  >
                    <SelectTrigger id="type">
                      <SelectValue placeholder={!selectedModelId ? "Select a model first" : "Select type"} />
                    </SelectTrigger>
                    <SelectContent>
                      {/* Show types available for the selected model name */}
                      {selectedModelId && (() => {
                        const selectedModel = availableModels.find(m => m.model_id === selectedModelId);
                        const modelName = selectedModel?.model_name;
                        const typesForModel = availableModels
                          .filter(m => m.model_name === modelName && m.type)
                          .map(m => m.type as string);
                        const uniqueTypes = Array.from(new Set(typesForModel));

                        return uniqueTypes.map((type) => (
                          <SelectItem key={type} value={type}>
                            {type}
                          </SelectItem>
                        ));
                      })()}
                    </SelectContent>
                  </Select>
                </div>

                <div className="space-y-2">
                  <Label htmlFor="start-date">Start Date (optional)</Label>
                  <Input
                    id="start-date"
                    type="date"
                    value={startDate}
                    onChange={(e) => setStartDate(e.target.value)}
                  />
                </div>

                <div className="space-y-2">
                  <Label htmlFor="end-date">End Date (optional)</Label>
                  <Input
                    id="end-date"
                    type="date"
                    value={endDate}
                    onChange={(e) => setEndDate(e.target.value)}
                  />
                </div>
              </div>

              <div className="space-y-2">
                <Label htmlFor="comment">Comment (optional)</Label>
                <textarea
                  id="comment"
                  value={comment}
                  onChange={(e) => setComment(e.target.value)}
                  className="w-full min-h-[80px] rounded-md border border-input bg-transparent px-3 py-2 text-sm"
                  maxLength={500}
                />
              </div>

              <Button type="submit" loading={isLoading} className="w-full">
                <IconCheck size={18} />
                Activate Vehicle
              </Button>
            </form>
          )}


          {/* CSV Import Form */}
          {inputMethod === 'csv' && (
            <div className="w-full rounded-lg border bg-white p-6 space-y-4">
              <div className="flex items-center justify-between gap-4">
                <div className="bg-blue-50 border border-blue-200 rounded-md p-3 flex-1">
                  <p className="text-sm text-blue-800 font-medium mb-2">
                    Expected CSV format:
                  </p>
                  <code className="text-xs block">
                    vin,make,model,type,start_date,end_date,comment
                  </code>
                </div>

                <div className="flex gap-2 flex-shrink-0">
                  <Button
                    onClick={downloadCSVTemplate}
                    variant="outline"
                    size="sm"
                    type="button"
                  >
                    <IconDownload size={16} className="mr-1" />
                    Download CSV
                  </Button>
                  {showValidation && csvValidation.length > 0 && (
                    <Button
                      onClick={clearCSVData}
                      variant="outline"
                      size="sm"
                    >
                      Clear CSV
                    </Button>
                  )}
                </div>
              </div>

              <div className="space-y-2">
                <Label htmlFor="csv-upload">Upload CSV File</Label>
                <div className="flex items-center gap-4">
                  <Input
                    id="csv-upload"
                    type="file"
                    accept=".csv"
                    onChange={handleCSVUpload}
                  />
                  <IconUpload size={24} className="text-gray-400" />
                </div>
              </div>

              {isValidatingCSV && (
                <div className="flex items-center justify-center p-8">
                  <div className="text-center">
                    <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary mx-auto mb-4"></div>
                    <p className="text-sm text-gray-600">Validating CSV data...</p>
                  </div>
                </div>
              )}

              {showValidation && csvValidation.length > 0 && (
                <>
                  <div>
                    <div className="flex items-center justify-between mb-3">
                      <p className="font-semibold">
                        Validation Results ({csvValidation.length} vehicles)
                      </p>
                      <div className="flex gap-2 text-xs">
                        <span className="px-2 py-1 bg-green-100 text-green-700 rounded">
                          ✓ {csvValidation.filter(r => r.status === 'valid').length} Valid
                        </span>
                        <span className="px-2 py-1 bg-yellow-100 text-yellow-700 rounded">
                          ⚠ {csvValidation.filter(r => r.status === 'suggestion').length} Suggestions
                        </span>
                        <span className="px-2 py-1 bg-red-100 text-red-700 rounded">
                          ✗ {csvValidation.filter(r => r.status === 'error').length} Errors
                        </span>
                      </div>
                    </div>

                    <div className="max-h-[400px] overflow-auto border rounded-md">
                      <table className="w-full text-sm">
                        <thead className="bg-gray-50 sticky top-0">
                          <tr className="border-b">
                            <th className="text-left py-2 px-3 font-medium">Status</th>
                            <th className="text-left py-2 px-3 font-medium">VIN</th>
                            <th className="text-left py-2 px-3 font-medium">Make</th>
                            <th className="text-left py-2 px-3 font-medium">Model</th>
                            <th className="text-left py-2 px-3 font-medium">Type</th>
                            <th className="text-left py-2 px-3 font-medium">Start Date</th>
                            <th className="text-left py-2 px-3 font-medium">End Date</th>
                            <th className="text-left py-2 px-3 font-medium">Actions</th>
                          </tr>
                        </thead>
                        <tbody>
                          {csvValidation.map((row, idx) => (
                            <tr key={idx} className={`border-b ${
                              row.status === 'error' ? 'bg-red-50' :
                              row.status === 'suggestion' ? 'bg-yellow-50' :
                              'bg-green-50'
                            }`}>
                              <td className="py-2 px-3">
                                {row.status === 'valid' && (
                                  <span className="text-green-600 font-bold">✓</span>
                                )}
                                {row.status === 'suggestion' && (
                                  <span className="text-yellow-600 font-bold">⚠</span>
                                )}
                                {row.status === 'error' && (
                                  <span className="text-red-600 font-bold">✗</span>
                                )}
                              </td>
                              <td className="py-2 px-3 font-mono text-xs">{row.vin}</td>
                              <td className="py-2 px-3">
                                <div>
                                  {/* Show original make if different */}
                                  {row.originalRow.make !== row.make && (
                                    <div className="text-gray-500 line-through text-xs">
                                      {row.originalRow.make}
                                    </div>
                                  )}

                                  {/* Show dropdown if make is invalid */}
                                  {!row.makeValid ? (
                                    <Select
                                      value={row.makeId || ''}
                                      onValueChange={(value) => updateMake(idx, value)}
                                    >
                                      <SelectTrigger className="h-8 text-xs">
                                        <SelectValue>
                                          <span className="text-red-600">{row.make}</span>
                                        </SelectValue>
                                      </SelectTrigger>
                                      <SelectContent>
                                        {availableMakes.map((make) => (
                                          <SelectItem key={make.make_id} value={make.make_id}>
                                            {make.make_name}
                                          </SelectItem>
                                        ))}
                                      </SelectContent>
                                    </Select>
                                  ) : (
                                    <span className="text-green-600 font-medium">
                                      {row.make}
                                    </span>
                                  )}
                                </div>
                              </td>
                              <td className="py-2 px-3">
                                <div>
                                  {row.originalRow.model !== row.model && (
                                    <div className="text-gray-500 line-through text-xs">
                                      {row.originalRow.model}
                                    </div>
                                  )}

                                  {/* Show dropdown if there are available models */}
                                  {row.availableModels && row.availableModels.length > 0 && (row.status === 'suggestion' || row.status === 'valid') ? (
                                    <Select
                                      value={row.suggestedModel?.model_id}
                                      onValueChange={(value) => updateModel(idx, value)}
                                    >
                                      <SelectTrigger className="h-8 text-xs">
                                        <SelectValue>
                                          {row.model}
                                        </SelectValue>
                                      </SelectTrigger>
                                      <SelectContent>
                                        {/* Get unique model names */}
                                        {Array.from(
                                          new Map(
                                            row.availableModels.map(model => [model.model_name, model])
                                          ).values()
                                        ).map((model) => (
                                          <SelectItem key={model.model_id} value={model.model_id}>
                                            {model.model_name}
                                          </SelectItem>
                                        ))}
                                      </SelectContent>
                                    </Select>
                                  ) : (
                                    <>
                                      {row.suggestedModel && (
                                        <div className="font-medium text-green-600">
                                          {row.model}
                                        </div>
                                      )}
                                      {!row.suggestedModel && (
                                        <div>{row.model}</div>
                                      )}
                                    </>
                                  )}
                                </div>
                              </td>
                              <td className="py-2 px-3">
                                <div>
                                  {row.originalRow.type && row.originalRow.type !== row.type && (
                                    <div className="text-gray-500 line-through text-xs">
                                      {row.originalRow.type}
                                    </div>
                                  )}

                                  {/* Show dropdown if there are available types */}
                                  {row.availableTypes && row.availableTypes.length > 0 && row.status === 'suggestion' ? (
                                    <Select
                                      value={row.type}
                                      onValueChange={(value) => updateType(idx, value)}
                                    >
                                      <SelectTrigger className="h-8 text-xs">
                                        <SelectValue placeholder="Select type" />
                                      </SelectTrigger>
                                      <SelectContent>
                                        {row.availableTypes.map((type) => (
                                          <SelectItem key={type} value={type}>
                                            {type}
                                          </SelectItem>
                                        ))}
                                      </SelectContent>
                                    </Select>
                                  ) : (
                                    <>
                                      {row.suggestedType && row.status === 'valid' && (
                                        <div className="font-medium text-green-600">
                                          {row.type || row.suggestedType}
                                        </div>
                                      )}
                                      {!row.suggestedType && row.type && (
                                        <div>{row.type}</div>
                                      )}
                                      {!row.type && !row.suggestedType && (
                                        <div className="text-gray-400 text-xs">No type</div>
                                      )}
                                    </>
                                  )}
                                </div>
                              </td>
                              <td className="py-2 px-3">
                                <Input
                                  type="date"
                                  value={row.startDate}
                                  onChange={(e) => updateDate(idx, 'start', e.target.value)}
                                  className={`h-8 text-xs ${!row.startDateValid ? 'border-red-500' : ''}`}
                                />
                                {!row.startDateValid && (
                                  <div className="text-xs text-red-600 mt-1">Invalid format</div>
                                )}
                              </td>
                              <td className="py-2 px-3">
                                <Input
                                  type="date"
                                  value={row.endDate}
                                  onChange={(e) => updateDate(idx, 'end', e.target.value)}
                                  className={`h-8 text-xs ${!row.endDateValid ? 'border-red-500' : ''}`}
                                />
                                {!row.endDateValid && (
                                  <div className="text-xs text-red-600 mt-1">Invalid format</div>
                                )}
                              </td>
                              <td className="py-2 px-3">
                                {row.status === 'suggestion' && (
                                  <div className="flex gap-1">
                                    <button
                                      onClick={() => acceptSuggestion(idx)}
                                      className="px-2 py-1 text-xs bg-green-600 text-white rounded hover:bg-green-700"
                                      title="Accept suggestion"
                                    >
                                      ✓ Accept
                                    </button>
                                    <button
                                      onClick={() => rejectSuggestion(idx)}
                                      className="px-2 py-1 text-xs bg-red-600 text-white rounded hover:bg-red-700"
                                      title="Reject suggestion"
                                    >
                                      ✗ Reject
                                    </button>
                                  </div>
                                )}
                                {row.status === 'error' && row.errorMessage && (
                                  <div className="text-xs text-red-600">
                                    {row.errorMessage}
                                  </div>
                                )}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>

                  <Button
                    onClick={handleCSVActivation}
                    loading={isLoading}
                    className="w-full"
                    disabled={csvValidation.filter(r => r.status === 'valid').length === 0}
                  >
                    <IconCheck size={18} />
                    Activate {csvValidation.filter(r => r.status === 'valid').length} Valid Vehicle{csvValidation.filter(r => r.status === 'valid').length !== 1 ? 's' : ''}
                    {csvValidation.filter(r => r.status !== 'valid').length > 0 && (
                      <span className="ml-2 text-xs opacity-75">
                        ({csvValidation.filter(r => r.status !== 'valid').length} skipped)
                      </span>
                    )}
                  </Button>
                </>
              )}
            </div>
          )}

          {/* Results */}
          {results && results.length > 0 && (
            <div className="w-full rounded-lg border bg-white p-6 space-y-4">
              <div className="flex items-center justify-between">
                <h3 className="font-semibold text-lg text-gray-900">
                  Activation Results
                </h3>
                <div className="flex gap-2 text-xs">
                  <span className="px-2 py-1 bg-green-100 text-green-700 rounded">
                    ✓ {results.filter(r => r.requested_status && r.status === true).length} Success
                  </span>
                  <span className="px-2 py-1 bg-green-100 text-green-700 rounded">
                    ✓ {results.filter(r => r.requested_status && r.status === false).length} Success
                  </span>
                  <span className="px-2 py-1 bg-yellow-100 text-yellow-700 rounded">
                    ⏳ {results.filter(r => r.requested_status && r.status === null).length} Pending
                  </span>
                  <span className="px-2 py-1 bg-red-100 text-red-700 rounded">
                    ✗ {results.filter(r => !r.requested_status || r.status === false).length} Failed
                  </span>
                </div>
              </div>

              <div className="overflow-x-auto">
                <table className="w-full text-sm bg-white rounded-md overflow-hidden border">
                  <thead className="bg-gray-50">
                    <tr className="border-b">
                      <th className="text-left py-3 px-4 font-medium">VIN</th>
                      <th className="text-center py-3 px-4 font-medium">Requested</th>
                      <th className="text-center py-3 px-4 font-medium">Status</th>
                      <th className="text-left py-3 px-4 font-medium">Backend Message</th>
                    </tr>
                  </thead>
                  <tbody>
                    {results.map((result, idx) => (
                      <tr key={idx} className={`border-b last:border-0 ${
                        !result.requested_status || result.status === false ? 'bg-red-50' :
                        result.status === null ? 'bg-yellow-50' :
                        'bg-green-50'
                      }`}>
                        <td className="py-3 px-4 font-mono text-xs">{result.vin}</td>
                        <td className="py-3 px-4 text-center">
                          {result.requested_status ? (
                            <span className="inline-block px-2 py-1 rounded bg-blue-100 text-blue-700 text-xs font-medium">
                              Yes
                            </span>
                          ) : (
                            <span className="inline-block px-2 py-1 rounded bg-gray-100 text-gray-600 text-xs font-medium">
                              No
                            </span>
                          )}
                        </td>
                        <td className="py-3 px-4 text-center">
                          {result.status === true ? (
                            <span className="inline-block px-2 py-1 rounded bg-green-100 text-green-700 text-xs font-medium">
                              ✓ Activated
                            </span>
                          ) : result.status === false ? (
                            <span className="inline-block px-2 py-1 rounded bg-red-100 text-red-700 text-xs font-medium">
                              ✗ Failed
                            </span>
                          ) : (
                            <span className="inline-block px-2 py-1 rounded bg-yellow-100 text-yellow-700 text-xs font-medium">
                              ⏳ Pending
                            </span>
                          )}
                        </td>
                        <td className="py-3 px-4">
                          <span className={`text-sm ${
                            !result.requested_status || result.status === false ? 'text-red-700 font-medium' :
                            result.status === null ? 'text-yellow-700' :
                            'text-green-700'
                          }`}>
                            {result.message}
                          </span>
                          {result.comment && (
                            <span className="text-xs text-gray-500 ml-2">
                              ({result.comment})
                            </span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
};

export default ActivateTab;
