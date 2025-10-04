// Warning: This file (and any imports) are included in the main bundle with Grafana in order to provide link extension support in Grafana core, in an effort to keep Grafana loading quickly, please do not add any unnecessary imports to this file and run the bundle analyzer before committing any changes!
import { DataFrame, DataSourceJsonData, ScopedVars, TimeRange } from '@grafana/data';
import { DataSourceWithBackend } from '@grafana/runtime';
import { DataSourceRef } from '@grafana/schema';

import { LabelType } from './fieldsTypes';

export enum LogsQueryDirection {
  Backward = 'backward',
  Forward = 'forward',
  Scan = 'scan',
}

export type LogsQuery = {
  datasource?: DataSourceRef;
  direction?: LogsQueryDirection;
  editorMode?: string;
  expr: string;
  legendFormat?: string;
  maxLines?: number;
  queryType?: LogsQueryType;
  refId: string;
  step?: string;
  supportingQueryType?: string;
};

export type LogsQueryType = 'instant' | 'range' | 'stream' | string;

export type LogsDatasource = DataSourceWithBackend<LogsQuery, DataSourceJsonData> & {
  maxLines?: number;
} & {
  getTimeRangeParams: (timeRange: TimeRange) => { end: number; start: number };
  // @todo delete after min supported grafana is upgraded to >=11.6
  interpolateString?: (string: string, scopedVars?: ScopedVars) => string;
};

export function getLabelTypeFromFrame(labelKey: string, frame: DataFrame, index = 0): null | LabelType {
  const typeField = frame.fields.find((field) => field.name === 'labelTypes')?.values[index];
  if (!typeField) {
    return null;
  }
  switch (typeField[labelKey]) {
    case 'I':
      return LabelType.Indexed;
    case 'S':
      return LabelType.StructuredMetadata;
    case 'P':
      return LabelType.Parsed;
    default:
      return null;
  }
}