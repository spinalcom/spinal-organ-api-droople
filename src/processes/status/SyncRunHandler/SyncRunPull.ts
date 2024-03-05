/*
 * Copyright 2021 SpinalCom - www.spinalcom.com
 *
 * This file is part of SpinalCore.
 *
 * Please read all of the following terms and conditions
 * of the Free Software license Agreement ("Agreement")
 * carefully.
 *
 * This Agreement is a legally binding contract between
 * the Licensee (as defined below) and SpinalCom that
 * sets forth the terms and conditions that govern your
 * use of the Program. By installing and/or using the
 * Program, you agree to abide by all the terms and
 * conditions stated or referenced herein.
 *
 * If you do not agree to abide by these terms and
 * conditions, do not demonstrate your acceptance and do
 * not install or use the Program.
 * You should have received a copy of the license along
 * with this file. If not, see
 * <http://resources.spinalcom.com/licenses.pdf>.
 */

import moment = require('moment');
import {
  SpinalContext,
  SpinalGraph,
  SpinalGraphService,
  SpinalNode,
  SpinalNodeRef,
  SPINAL_RELATION_PTR_LST_TYPE,
} from 'spinal-env-viewer-graph-service';
import { spinalServiceTicket } from 'spinal-service-ticket';
import {
  SPINAL_TICKET_SERVICE_STEP_RELATION_NAME,
  SPINAL_TICKET_SERVICE_STEP_TYPE,
  SPINAL_TICKET_SERVICE_TICKET_RELATION_NAME,
  SPINAL_TICKET_SERVICE_PROCESS_RELATION_NAME,
  SPINAL_TICKET_SERVICE_TICKET_TYPE,
} from '../../../constants';
import type OrganConfigModel from '../../../model/OrganConfigModel';
import {
  IAssetResponse,
  IDevice,
  IAsset,
  getAssets
} from '../../../services/client/DIConsulte';
import { attributeService } from 'spinal-env-viewer-plugin-documentation-service';
import { NetworkService } from 'spinal-model-bmsnetwork';
import {
  InputDataDevice,
  InputDataEndpoint,
  InputDataEndpointGroup,
  InputDataEndpointDataType,
  InputDataEndpointType,
} from '../../../model/InputData/InputDataModel/InputDataModel';
import { SpinalServiceTimeseries } from 'spinal-model-timeseries';

/**
 * Main purpose of this class is to pull tickets from client.
 *
 * @export
 * @class SyncRunPull
 */
export class SyncRunPull {
  graph: SpinalGraph<any>;
  config: OrganConfigModel;
  interval: number;
  running: boolean;
  nwService: NetworkService;
  timeseriesService: SpinalServiceTimeseries;
  mappingElevators: Map<string, string>;

  constructor(
    graph: SpinalGraph<any>,
    config: OrganConfigModel,
    nwService: NetworkService
  ) {
    this.graph = graph;
    this.config = config;
    this.running = false;
    this.nwService = nwService;
    this.timeseriesService = new SpinalServiceTimeseries();
    this.mappingElevators = new Map<string, string>();
  }

  async getSpinalGeo(): Promise<SpinalContext<any>> {
    const contexts = await this.graph.getChildren();
    for (const context of contexts) {
      if (context.info.id.get() === this.config.spatialContextId?.get()) {
        // @ts-ignore
        SpinalGraphService._addNode(context);
        return context;
      }
    }
    const context = await this.graph.getContext('spatial');
    if (!context) throw new Error('Context Not found');
    return context;
  }

  async getNetworkContext(): Promise<SpinalNode<any>> {
    const contexts = await this.graph.getChildren();
    for (const context of contexts) {
      if (context.info.name.get() === process.env.NETWORK_CONTEXT_NAME) {
        // @ts-ignore
        SpinalGraphService._addNode(context);
        return context;
      }
    }
    throw new Error('Network Context Not found');
  }

  private waitFct(nb: number): Promise<void> {
    return new Promise((resolve) => {
      setTimeout(
        () => {
          resolve();
        },
        nb >= 0 ? nb : 0
      );
    });
  }



  dateToNumber(dateString: string | Date) {
    const dateObj = new Date(dateString);
    return dateObj.getTime();
  }





  /*
  async createDevicesIfNotExist() {
    const networkContext = await this.getNetworkContext();

    for (const elevator of this.foundElevators) {
      const devices = await networkContext.findInContext(
        networkContext,
        (node) => node.info.name.get() === elevator
      );
      if (devices.length > 0) {
        console.log('Device already exists, not creating ', elevator);
        continue;
      }
      
      const startDate = moment().subtract(120, 'days').format('YYYY-MM-DD');
      const endDate = moment().format('YYYY-MM-DD');

      try {
        const performanceData = await getPerformanceData(
          elevator,
          startDate,
          endDate
        );

        const statusData = await getStatusData(elevator);

        const device = new InputDataDevice(elevator, 'device');

        const uptime_30daysEndpoint = new InputDataEndpoint(
          `${elevator}_Uptime 30 days`,
          performanceData.uptime_30days,
          '%',
          InputDataEndpointDataType.Real,
          InputDataEndpointType.Other
        );
        device.children.push(uptime_30daysEndpoint);

        const elevatorPerformance = performanceData.performance;

        const runCountValue = parseInt(
          elevatorPerformance[elevatorPerformance.length - 1].run_counts
        );
        const runCountsEndpoint = new InputDataEndpoint(
          `${elevator}_Run counts`,
          runCountValue,
          '',
          InputDataEndpointDataType.Integer,
          InputDataEndpointType.Other
        );

        const doorCylcesValue = parseInt(
          elevatorPerformance[elevatorPerformance.length - 1].door_cycles
        );
        const doorCyclesEndpoint = new InputDataEndpoint(
          `${elevator}_Door cycles`,
          doorCylcesValue,
          '',
          InputDataEndpointDataType.Integer,
          InputDataEndpointType.Other
        );
        device.children.push(runCountsEndpoint);
        device.children.push(doorCyclesEndpoint);

        const unitStateEndpoint = new InputDataEndpoint(
          `${elevator}_Unit state`,
          statusData.unit_state,
          '',
          InputDataEndpointDataType.String,
          InputDataEndpointType.Other
        );

        const floorPositionEndpoint = new InputDataEndpoint(
          `${elevator}_Floor position`,
          statusData.floor,
          '',
          InputDataEndpointDataType.String,
          InputDataEndpointType.Other
        );

        const movementInfoEndpoint = new InputDataEndpoint(
          `${elevator}_Movement`,
          statusData.moving_direction,
          '',
          InputDataEndpointDataType.String,
          InputDataEndpointType.Other
        );

        const frontDoorStatusEndpoint = new InputDataEndpoint(
          `${elevator}_Front door status`,
          statusData.front_door_status,
          '',
          InputDataEndpointDataType.String,
          InputDataEndpointType.Other
        );

        const rearDoorStatusEndpoint = new InputDataEndpoint(
          `${elevator}_Rear door status`,
          statusData.rear_door_status,
          '',
          InputDataEndpointDataType.String,
          InputDataEndpointType.Other
        );
        device.children.push(unitStateEndpoint);
        device.children.push(floorPositionEndpoint);
        device.children.push(movementInfoEndpoint);
        device.children.push(frontDoorStatusEndpoint);
        device.children.push(rearDoorStatusEndpoint);

        console.log('Creating device');
        await this.nwService.updateData(device, this.dateToNumber(endDate));
        //await this.initData(networkContext, elevator, elevatorPerformance);
      }
    }
  } catch (e) {} */

  async createDevicesIfNotExist(assetData: IAsset[]) {
    const networkContext = await this.getNetworkContext();

    for (const asset of assetData) {
      for(const device of asset.devices) {
        console.log(device.dev_id);
        const devices = await networkContext.findInContext(
          networkContext,
          (node) => node.getName().get() == device.dev_id
        );
        if (devices.length > 0) {
          console.log('Device already exists, not creating ', device.dev_id);
          continue;
        }

        const deviceNode = new InputDataDevice(device.dev_id, 'device');
        await this.nwService.updateData(deviceNode)
        console.log('Device created :', device.dev_id);
      }
    }
  }

  async updateDeviceData(assetData : IAsset[]){
    const networkContext = await this.getNetworkContext();
    for(const asset of assetData){
      for(const device of asset.devices){
        const nodes = await networkContext.findInContext(
          networkContext,
          (node) => node.getName().get() === device.dev_id
        );
        if(nodes.length === 0){ throw new Error('Device not found');}
        const deviceNode = nodes[0];
        SpinalGraphService._addNode(deviceNode);
        const endpoints = await deviceNode.getChildren('hasBmsEndpoint');
        for(const telemetry of device.last_telemetry){
          if(telemetry.value === null) continue;
          const endpoint = endpoints.find((endpoint) => endpoint.getName().get() === `${telemetry.data_type}-${telemetry.id}`);
          
          if(!endpoint){
            //create new endpoint
            const newEndpoint = new InputDataEndpoint(`${telemetry.data_type}-${telemetry.id}`, telemetry.value, telemetry.unit, InputDataEndpointDataType.Real, InputDataEndpointType.Other);
            await this.nwService.createNewBmsEndpoint(deviceNode.info.id.get(),newEndpoint);
          }
          
          else{
            SpinalGraphService._addNode(endpoint);
            let timeseries = await this.timeseriesService.getOrCreateTimeSeries(
              endpoint.getId().get()
            );
            await timeseries.push(telemetry.value);
            //await this.timeseriesService.pushFromEndpoint(endpoint.getId().get(), telemetry.value);
            const model = await endpoint.element.load();
            model.currentValue.set(telemetry.value);
          }
            
        }



        }
      
    }
  }

  async init(): Promise<void> {
    console.log('Initiating SyncRunPull');
    try {
      const assetData = await getAssets();
      await this.createDevicesIfNotExist(assetData.data);
      await this.updateDeviceData(assetData.data);

      this.config.lastSync.set(Date.now());
    } catch (e) {
      console.error(e);
    }
  }

  async run(): Promise<void> {
    this.running = true;
    const timeout = parseInt(process.env.PULL_INTERVAL);
    await this.waitFct(timeout);
    while (true) {
      if (!this.running) break;
      const before = Date.now();
      try {
        console.log("Updating data...");
        const assetData = await getAssets();
      await this.createDevicesIfNotExist(assetData.data);
      await this.updateDeviceData(assetData.data);
        this.config.lastSync.set(Date.now());
      } catch (e) {
        
        console.error(e);
        await this.waitFct(1000 * 60);
      } finally {
        const delta = Date.now() - before;
        const timeout = parseInt(process.env.PULL_INTERVAL) - delta;
        await this.waitFct(timeout);
      }
    }

  }

  stop(): void {
    this.running = false;
  }
}
export default SyncRunPull;
