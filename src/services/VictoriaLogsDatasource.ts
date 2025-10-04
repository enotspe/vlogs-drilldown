import { Observable, Subscriber } from 'rxjs';

import {
  createDataFrame,
  DataQueryRequest,
  DataQueryResponse,
  Field,
  FieldType,
  LoadingState,
  TestDataSourceResponse,
} from '@grafana/data';
import { DataSourceWithBackend, getDataSourceSrv } from '@grafana/runtime';
import { RuntimeDataSource, sceneUtils } from '@grafana/scenes';
import { DataQuery } from '@grafana/schema';

import { SceneDataQueryRequest, SceneDataQueryResourceRequest, VolumeRequestProps } from './datasourceTypes';
import { DetectedFieldsResponse, DetectedLabelsResponse } from './fields';
import { FIELDS_TO_REMOVE, LABELS_TO_REMOVE, sortLabelsByCardinality } from './filters';
import { logger } from './logger';
import { PLUGIN_ID } from './plugin';
import { getDataSource } from './scenes';
import { SERVICE_NAME } from './variables';
import { VictoriaLogsExpressionBuilder } from './VictoriaLogsExpressionBuilder';

export type VictoriaLogsQuery = DataQuery & {
  expr: string;
};

export type VictoriaLogsDatasource = DataSourceWithBackend<VictoriaLogsQuery, any>;

export const WRAPPED_VICTORIALOGS_DS_UID = 'wrapped-victorialogs-ds-uid';

type TimeStampOfVolumeEval = number;
type VolumeCount = string;
type VolumeValue = [TimeStampOfVolumeEval, VolumeCount];
type VolumeResult = {
  metric: {
    [index: string]: string | undefined;
  };
  value: VolumeValue;
};

type IndexVolumeResponse = {
  data: {
    result: VolumeResult[];
  };
};

type LabelsResponse = {
  data: string[];
  status: string;
};

type SampleTimeStamp = number;
type SampleCount = number;
type PatternSample = [SampleTimeStamp, SampleCount];

export interface VictoriaLogsPattern {
  level?: string;
  pattern: string;
  samples: PatternSample[];
}

export interface VictoriaLogsPatternProcessed {
  levels: string[];
  samples: PatternSample[];
}

type PatternsResponse = {
  data: VictoriaLogsPattern[];
};

export const DETECTED_FIELDS_NAME_FIELD = 'name';
export const DETECTED_FIELDS_CARDINALITY_NAME = 'cardinality';
export const DETECTED_FIELDS_PARSER_NAME = 'parser';
export const DETECTED_FIELDS_TYPE_NAME = 'type';
export const DETECTED_FIELDS_PATH_NAME = 'jsonPath';
export const MAX_PATTERNS_LIMIT = 500;

export class WrappedVictoriaLogsDatasource extends RuntimeDataSource<DataQuery> {
  constructor(pluginId: string, uid: string) {
    super(pluginId, uid);
  }

  query(request: SceneDataQueryRequest): Promise<DataQueryResponse> | Observable<DataQueryResponse> {
    return new Observable<DataQueryResponse>((subscriber) => {
      if (!request.scopedVars?.__sceneObject) {
        throw new Error('Scene object not found in request');
      }

      getDataSourceSrv()
        .get(getDataSource(request.scopedVars.__sceneObject.valueOf()))
        .then(async (ds) => {
          if (!(ds instanceof DataSourceWithBackend) || !('interpolateString' in ds) || !('getTimeRangeParams' in ds)) {
            throw new Error('Invalid datasource!');
          }

          const victoriaLogsDs = ds as VictoriaLogsDatasource;

          request.targets = request.targets?.map((target) => {
            target.datasource = victoriaLogsDs;
            return target;
          });

          const targetsSet = new Set();
          request.targets.forEach((target) => {
            targetsSet.add(target.resource ?? '');
          });

          if (targetsSet.size !== 1) {
            throw new Error('A request cannot contain queries to multiple endpoints');
          }

          const requestType = request.targets[0].resource;

          switch (requestType) {
            case 'volume': {
              await this.getVolume(request, victoriaLogsDs, subscriber);
              break;
            }
            case 'patterns': {
              await this.getPatterns(request, victoriaLogsDs, subscriber);
              break;
            }
            case 'detected_labels': {
              await this.getDetectedLabels(request, victoriaLogsDs, subscriber);
              break;
            }
            case 'detected_fields': {
              await this.getDetectedFields(request, victoriaLogsDs, subscriber);
              break;
            }
            case 'labels': {
              await this.getLabels(request, victoriaLogsDs, subscriber);
              break;
            }
            default: {
              this.getData(request, victoriaLogsDs, subscriber);
              break;
            }
          }
        });
    });
  }

  private getData(request: SceneDataQueryRequest, ds: VictoriaLogsDatasource, subscriber: Subscriber<DataQueryResponse>) {
    const expressionBuilder = new VictoriaLogsExpressionBuilder(request.targets as any);
    const expr = expressionBuilder.getFilterExpr();

    const updatedRequest = {
      ...request,
      targets: ds.interpolateVariablesInQueries(request.targets, request.scopedVars).map((target) => ({
        ...target,
        expr: expr,
        resource: undefined,
      })),
    };

    const dsResponse = ds.query(updatedRequest);
    dsResponse.subscribe(subscriber);

    return subscriber;
  }

  private async getPatterns(
    request: DataQueryRequest<VictoriaLogsQuery & SceneDataQueryResourceRequest>,
    ds: VictoriaLogsDatasource,
    subscriber: Subscriber<DataQueryResponse>
  ) {
    const targets = request.targets.filter((target) => {
      return target.resource === 'detected_fields';
    });

    if (targets.length !== 1) {
      throw new Error('Detected fields query can only have a single target!');
    }

    subscriber.next({ data: [], state: LoadingState.Loading });

    const { expression, interpolatedTarget } = this.interpolate(ds, targets, request);

    try {
      const response = await ds.getResource<DetectedFieldsResponse>('field_names', {
        query: expression,
        start: request.range.from.utc().toISOString(),
        end: request.range.to.utc().toISOString(),
      });

      const nameField: Field = { config: {}, name: DETECTED_FIELDS_NAME_FIELD, type: FieldType.string, values: [] };
      const cardinalityField: Field = {
        config: {},
        name: DETECTED_FIELDS_CARDINALITY_NAME,
        type: FieldType.number,
        values: [],
      };
      const parserField: Field = { config: {}, name: DETECTED_FIELDS_PARSER_NAME, type: FieldType.string, values: [] };
      const typeField: Field = { config: {}, name: DETECTED_FIELDS_TYPE_NAME, type: FieldType.string, values: [] };
      const pathField: Field = { config: {}, name: DETECTED_FIELDS_PATH_NAME, type: FieldType.string, values: [] };

      for (const field of response.values) {
        if (!FIELDS_TO_REMOVE.includes(field.value)) {
          nameField.values.push(field.value);
          const valuesResponse = await ds.getResource('field_values', {
            query: expression,
            field: field.value,
            start: request.range.from.utc().toISOString(),
            end: request.range.to.utc().toISOString(),
          });
          cardinalityField.values.push(valuesResponse.values.length);
          parserField.values.push('unknown'); // VictoriaLogs does not provide this info
          typeField.values.push('string'); // VictoriaLogs does not provide this info
          pathField.values.push(''); // VictoriaLogs does not provide this info
        }
      }

      const dataFrame = createDataFrame({
        fields: [nameField, cardinalityField, parserField, typeField, pathField],
        refId: interpolatedTarget.refId,
      });

      subscriber.next({ data: [dataFrame], state: LoadingState.Done });
    } catch (e) {
      logger.error(e, { msg: 'Detected fields error' });
      subscriber.next({ data: [], state: LoadingState.Error });
    }

    return subscriber;
  }

  private interpolate(
    ds: VictoriaLogsDatasource,
    targets: Array<VictoriaLogsQuery & SceneDataQueryResourceRequest>,
    request: DataQueryRequest<VictoriaLogsQuery & SceneDataQueryResourceRequest>
  ) {
    const targetsInterpolated = ds.interpolateVariablesInQueries(targets, request.scopedVars);
    if (!targetsInterpolated.length) {
      throw new Error('Datasource failed to interpolate query!');
    }
    const interpolatedTarget = targetsInterpolated[0];
    const adHocFilters = request.adhocFilters || [];
    const expressionBuilder = new VictoriaLogsExpressionBuilder(adHocFilters);
    const expression = expressionBuilder.getLabelsExpr();
    return { expression, interpolatedTarget };
  }

  private async getDetectedLabels(
    request: DataQueryRequest<VictoriaLogsQuery & SceneDataQueryResourceRequest>,
    ds: VictoriaLogsDatasource,
    subscriber: Subscriber<DataQueryResponse>
  ) {
    const targets = request.targets.filter((target) => {
      return target.resource === 'detected_labels';
    });

    if (targets.length !== 1) {
      throw new Error('Detected labels query can only have a single target!');
    }

    let { expression, interpolatedTarget } = this.interpolate(ds, targets, request);

    subscriber.next({ data: [], state: LoadingState.Loading });

    try {
      const labelsResponse = await ds.getResource('labels', { query: expression });
      const labels = (labelsResponse as LabelsResponse).data
        ?.filter((label) => !LABELS_TO_REMOVE.includes(label))
        .sort();

      const detectedLabelFields: Array<Partial<Field>> =
        labels?.map((label) => {
          return {
            name: label,
            values: [], // Cardinality will be calculated in the next step
          };
        }) ?? [];

      for (const field of detectedLabelFields) {
        const valuesResponse = await ds.getResource('label_values', { query: expression, label: field.name });
        field.values = [(valuesResponse as LabelsResponse).data.length];
      }

      const dataFrame = createDataFrame({
        fields: detectedLabelFields ?? [],
        refId: interpolatedTarget.refId,
      });

      subscriber.next({ data: [dataFrame], state: LoadingState.Done });
    } catch (e) {
      subscriber.next({ data: [], state: LoadingState.Error });
    }

    return subscriber;
  }

  private async getDetectedFields(
    request: DataQueryRequest<VictoriaLogsQuery & SceneDataQueryResourceRequest>,
    ds: VictoriaLogsDatasource,
    subscriber: Subscriber<DataQueryResponse>
  ) {
    if (request.targets.length !== 1) {
      throw new Error('Volume query can only have a single target!');
    }

    const target = request.targets[0];
    const primaryLabel = target.primaryLabel;
    if (!primaryLabel) {
      throw new Error('Primary label is required for volume queries!');
    }

    const { expression, interpolatedTarget } = this.interpolate(ds, [target], request);
    const logsQL = `${expression} | stats count() by (${primaryLabel}) | sort by ("count()") desc | limit 5000`;

    subscriber.next({ data: [], state: LoadingState.Loading });

    try {
      const res = await ds.query({
        ...request,
        targets: [{ ...interpolatedTarget, expr: logsQL, refId: 'volume' }],
      }).toPromise();

      const frame = res?.data[0];
      if (frame) {
        const primaryLabelField = frame.fields.find((f: Field) => f.name === primaryLabel);
        if (primaryLabelField) {
          primaryLabelField.name = SERVICE_NAME;
        }
        const countField = frame.fields.find((f: Field) => f.name === 'count()');
        if (countField) {
          countField.name = 'volume';
        }
      }

      subscriber.next({ data: frame ? [frame] : [], state: LoadingState.Done });
    } catch (e) {
      logger.error(e);
      subscriber.next({ data: [], state: LoadingState.Error });
    }

    subscriber.complete();

    return subscriber;
  }

  //@todo doesn't work with multiple queries
  private async getVolume(
    request: DataQueryRequest<VictoriaLogsQuery & SceneDataQueryResourceRequest & VolumeRequestProps>,
    ds: VictoriaLogsDatasource,
    subscriber: Subscriber<DataQueryResponse>
  ) {
    const targets = request.targets.filter((target) => {
      return target.resource === 'patterns';
    });

    if (targets.length !== 1) {
      throw new Error('Patterns query can only have a single target!');
    }
    const { expression, interpolatedTarget } = this.interpolate(ds, targets, request);
    const logsQL = `${expression} | collapse_nums | top ${MAX_PATTERNS_LIMIT} by (_msg)`;

    subscriber.next({ data: [], state: LoadingState.Loading });

    try {
      const res = await ds.query({
        ...request,
        targets: [{ ...interpolatedTarget, expr: logsQL, refId: 'patterns' }],
      }).toPromise();

      const frame = res?.data[0];
      if (frame) {
        const msgField = frame.fields.find((f: Field) => f.name === '_msg');
        const hitsField = frame.fields.find((f: Field) => f.name === 'hits');

        if (msgField && hitsField) {
          const frames = [];
          for (let i = 0; i < msgField.values.length; i++) {
            const pattern = msgField.values.get(i);
            const hits = hitsField.values.get(i);
            const newFrame = createDataFrame({
              fields: [
                {
                  config: {},
                  name: 'time',
                  type: FieldType.time,
                  values: [request.range.from.valueOf()],
                },
                {
                  config: {},
                  name: pattern,
                  type: FieldType.number,
                  values: [hits],
                },
              ],
              meta: {
                custom: {
                  sum: hits,
                  level: [], // VictoriaLogs does not provide level info for patterns
                },
                preferredVisualisationType: 'graph',
              },
              name: pattern,
              refId: interpolatedTarget.refId,
            });
            frames.push(newFrame);
          }
          subscriber.next({ data: frames, state: LoadingState.Done });
        } else {
          subscriber.next({ data: [], state: LoadingState.Done });
        }
      } else {
        subscriber.next({ data: [], state: LoadingState.Done });
      }
    } catch (e) {
      logger.error(e, { msg: 'Patterns error' });
      subscriber.next({ data: [], state: LoadingState.Error });
    }

    return subscriber;
  }

  private async getLabels(
    request: DataQueryRequest<VictoriaLogsQuery & SceneDataQueryResourceRequest>,
    ds: VictoriaLogsDatasource,
    subscriber: Subscriber<DataQueryResponse>
  ) {
    // Implement with VictoriaLogs API
    subscriber.next({ data: [], state: LoadingState.Done });
  }

  testDatasource(): Promise<TestDataSourceResponse> {
    return Promise.resolve({ message: 'Data source is working', status: 'success', title: 'Success' });
  }
}

let initialized = false;
function init() {
  if (initialized) {
    return;
  }
  initialized = true;
  sceneUtils.registerRuntimeDataSource({
    dataSource: new WrappedVictoriaLogsDatasource('wrapped-victorialogs-ds', WRAPPED_VICTORIALOGS_DS_UID),
  });
}

export default init;