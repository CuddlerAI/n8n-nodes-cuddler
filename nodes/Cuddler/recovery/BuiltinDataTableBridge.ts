import type { IDataObject, IDataTableProjectService, IExecuteFunctions, INodeParameters, IWorkflowBase, DataTableRowReturn } from 'n8n-workflow';
import { NodeConnectionTypes } from 'n8n-workflow';

export interface ChildObservation { operation: string; executionId: string }

/** Fixed real built-in child graph, never caller-supplied code or node identity. */
function graph(parentId: string, parameters: INodeParameters): IWorkflowBase {
  return {
    id: parentId, name: 'Cuddler private fixed DataTable CAS child', active: false,
    isArchived: false, activeVersionId: null, createdAt: new Date(0), updatedAt: new Date(0),
    nodes: [
      { id: 'start', name: 'Subworkflow Input', type: 'n8n-nodes-base.executeWorkflowTrigger', typeVersion: 1.1,
        position: [0, 0], parameters: { inputSource: 'passthrough' } },
      { id: 'datatable', name: 'Data Table', type: 'n8n-nodes-base.dataTable', typeVersion: 1.1,
        position: [240, 0], parameters },
    ],
    connections: { 'Subworkflow Input': { main: [[{ node: 'Data Table', type: NodeConnectionTypes.Main, index: 0 }]] } },
    settings: { executionOrder: 'v1', saveDataSuccessExecution: 'all', saveDataErrorExecution: 'all', saveManualExecutions: true },
  };
}

/** Fixed built-in adapter; all plan bytes passed to it must already be encrypted. */
export class BuiltinDataTableBridge {
  constructor(private readonly context: IExecuteFunctions, private readonly children: ChildObservation[]) {}

  private async call(operation: string, parameters: INodeParameters): Promise<IDataObject[]> {
    const workflowId = this.context.getWorkflow().id;
    const executionId = this.context.getExecutionId();
    if (!workflowId || !executionId) throw new Error('Trusted parent identity unavailable');
    const result = await this.context.executeWorkflow({ code: graph(workflowId, parameters) }, [{ json: {} }], undefined, {
      doNotWaitToFinish: false, executionMode: this.context.getMode(), parentExecution: { executionId, workflowId },
    });
    if (!result.executionId || result.waitTill || result.data.length !== 1) throw new Error('Unexpected actual child execution result');
    this.children.push({ operation, executionId: result.executionId });
    return (result.data[0] ?? []).map((item) => item.json);
  }

  private locator(tableId: string) {
    if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(tableId)) throw new Error('Invalid native table identity');
    return { __rl: true, value: tableId, mode: 'id' };
  }

  private validRow(row: IDataObject): boolean {
    return typeof row.id === 'number' && Number.isSafeInteger(row.id) && row.id > 0 && typeof row.etag === 'string' && typeof row.payload === 'string';
  }

  capability(tableId: string): Pick<IDataTableProjectService, 'getManyRowsAndCount' | 'updateRows'> {
    const locator = this.locator(tableId);
    return {
      getManyRowsAndCount: async (options) => {
        const filters = options.filter?.filters;
        if (options.filter?.type !== 'and' || filters?.length !== 1 || filters[0].columnName !== 'id' || filters[0].condition !== 'eq' ||
            typeof filters[0].value !== 'number' || !Number.isSafeInteger(filters[0].value) || filters[0].value < 1 || options.take !== 2) throw new Error('Unsupported ledger read');
        const rows = await this.call('row/get', { resource: 'row', operation: 'get', dataTableId: locator, matchType: 'allConditions',
          filters: { conditions: [{ keyName: 'id', condition: 'eq', keyValue: filters[0].value }] }, returnAll: false, limit: 2, orderBy: false });
        if (rows.length > 1 || rows.some((row) => !this.validRow(row))) throw new Error('Ambiguous or malformed actual ledger row');
        // Only an equality query on the system primary key is accepted here, so
        // rows.length is the complete cardinality, never a truncated page count.
        return { count: rows.length, data: rows as unknown as DataTableRowReturn[] };
      },
      updateRows: async (options) => {
        const filters = options.filter.filters;
        if (options.filter.type !== 'and' || filters.length !== 2 || filters[0].columnName !== 'id' || filters[0].condition !== 'eq' ||
            typeof filters[0].value !== 'number' || !Number.isSafeInteger(filters[0].value) || filters[0].value < 1 ||
            filters[1].columnName !== 'etag' || filters[1].condition !== 'eq' || typeof filters[1].value !== 'string' || !/^[a-f0-9]{64}$/.test(filters[1].value) ||
            typeof options.data.etag !== 'string' || !/^[a-f0-9]{64}$/.test(options.data.etag) || typeof options.data.payload !== 'string' || options.dryRun) throw new Error('Unsupported ledger CAS');
        // These are actual built-in allConditions fields, reaching the same
        // final SQL UPDATE filter. Native update output is not a success proof.
        const rows = await this.call('row/update', { resource: 'row', operation: 'update', dataTableId: locator, matchType: 'allConditions',
          filters: { conditions: [
            { keyName: 'id', condition: 'eq', keyValue: filters[0].value },
            { keyName: 'etag', condition: 'eq', keyValue: filters[1].value },
          ] },
          columns: { mappingMode: 'defineBelow', value: { etag: options.data.etag, payload: options.data.payload } }, options: { dryRun: false } });
        return rows as unknown as DataTableRowReturn[];
      },
    };
  }
}
