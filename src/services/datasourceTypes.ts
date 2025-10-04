import { DataQueryRequest } from '@grafana/data';
import { SceneObject } from '@grafana/scenes';

import { LogsQuery } from './queryTypes';

export type SceneDataQueryRequest = DataQueryRequest<LogsQuery & SceneDataQueryResourceRequest & VolumeRequestProps> & {
  scopedVars?: { __sceneObject?: { valueOf: () => SceneObject } };
};
export type SceneDataQueryResourceRequest = {
  resource?: SceneDataQueryResourceRequestOptions;
};

export type SceneDataQueryResourceRequestOptions =
  | 'detected_fields'
  | 'detected_labels'
  | 'labels'
  | 'patterns'
  | 'volume';

export type VolumeRequestProps = {
  primaryLabel?: string;
};
