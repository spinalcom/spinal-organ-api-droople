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
import type OrganConfigModel from '../../../model/OrganConfigModel';
import {
  IAssetResponse,
  ILastTelemetry,
  IDevice,
  IAsset,
  getAssets,
  INotificationResponse,
  getNotifications

} from '../../../services/client/DIConsulte';
import serviceDocumentation, { attributeService } from 'spinal-env-viewer-plugin-documentation-service';
import { NetworkService, SpinalBmsEndpoint } from 'spinal-model-bmsnetwork';
import {
  InputDataDevice,
  InputDataEndpoint,
  InputDataEndpointGroup,
  InputDataEndpointDataType,
  InputDataEndpointType,
} from '../../../model/InputData/InputDataModel/InputDataModel';
import { SpinalServiceTimeseries } from 'spinal-model-timeseries';
import { spinalServiceTicket } from 'spinal-service-ticket';
import { stringToTimestamp } from '../../../utils/DateString';
import { Str } from 'spinal-core-connectorjs';
import { type } from 'os';

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
  networkContext: SpinalNode<any>;
  virtualNetworkContext : SpinalNode<any>;
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
  
  async getBimContext(): Promise<SpinalNode<any>> {
    const contexts = await this.graph.getChildren();
    for (const context of contexts) {
      if (context.info.name.get() === process.env.BIM_CONTEXT_NAME) {
        
        SpinalGraphService._addNode(context);
        return context;
      }
    }
    throw new Error('bim Context Not found');
  }
  async getTicketContext(): Promise<SpinalNode<any>> {
    const contexts = await this.graph.getChildren();
    for (const context of contexts) {
      if (context.info.name.get() === process.env.TICKET_CONTEXT_NAME) {
        // @ts-ignore
        SpinalGraphService._addNode(context);
        return context;
      }
    }
    throw new Error('Ticket Context Not found');
  }

  async getTicketProcess(): Promise<SpinalNode<any>> {
    const context = await this.getTicketContext();
    const processes = await context.getChildren('SpinalSystemServiceTicketHasProcess');
    const ticketProcess = processes.find((proc) => {
      // @ts-ignore
      SpinalGraphService._addNode(proc);
      return proc.getName().get() === process.env.TICKET_PROCESS_NAME;
    });
    if (!ticketProcess) {
      throw new Error('Ticket Process Not found');
    }
    return ticketProcess;
  }


  async getVirtualNetwork(): Promise<SpinalNode<any>> {
    const context = await this.getNetworkContext();
    const virtualNetworks = await context.getChildren('hasBmsNetwork');
    return virtualNetworks.find((network) => {
      return network.getName().get() === process.env.VIRTUAL_NETWORK_NAME;
    })
    
  }

  async initNetworkNodes(): Promise<void> {
    const context = await this.getNetworkContext();
    const virtualNetwork = await this.getVirtualNetwork();
    this.networkContext = context;
    this.virtualNetworkContext = virtualNetwork;
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
async getBimObjects(): Promise<SpinalNodeRef[]> {
      
            const Context = await this.getBimContext();
            if (!Context) {
                console.log("BIM Context not found");
                return [];
            }
            const ContextID = Context.info.id.get();
            const category = (await SpinalGraphService.getChildren(ContextID, ["hasCategory"])).find(child => child.name.get() === process.env.BIM_CATEGORY_NAME);
            if (!category) {
                console.log("BIM Category not found");
                return [];
            }
            const categoryID = category.id.get();
            const Groups = await SpinalGraphService.getChildren(categoryID, ["hasGroup"]);
            if (Groups.length === 0) {
                console.log("No groups found under the category");
                return [];
            }
            const Group = Groups.find(G => G.name.get() === process.env.BIM_GROUP_NAME);
            if (!Group) {
                console.log("BIM Group not found");
                return [];
            }
            const BimObjects= await SpinalGraphService.getChildren(Group.id.get(), ["groupHasBIMObject"]);
            if (BimObjects.length === 0) {
                console.log("No BimObject found in the group");
                return [];
            }         
            return BimObjects;       
    }
  async pullAndUpdateTickets(): Promise<void> {
    
    const context = await this.getTicketContext();
    const processNode = await this.getTicketProcess();


    const steps: SpinalNodeRef[] =
      await spinalServiceTicket.getStepsFromProcess(
        processNode.getId().get(),
        context.getId().get()
      );

    const raisedStep = steps.find((step) => {
      return step.name.get() === 'Raised';
    });

    const solvedStep = steps.find((step) => {
      return step.name.get() === 'Solved';
    })


    const raisedTickets = await spinalServiceTicket.getTicketsFromStep(
      raisedStep.id.get()
    );
    const solvedTickets = await spinalServiceTicket.getTicketsFromStep(
      solvedStep.id.get()
    );


    console.log('Current Raised Tickets:', raisedTickets.length);
    console.log('Current Solved Tickets:', solvedTickets.length);


    

    const notificationData = await getNotifications(); // Updated to fetch notifications
    console.log('Fetched notifications:', notificationData);

    for(const notification of notificationData.data) {
      const ticketInfo = {
        name : `${notification.type}-${notification.entity.asset.id}`,
        date: notification.date,
        client_name: notification.client_name,
        entity_class: notification.entity_class,
      };
      console.log('Ticket:', ticketInfo);
      


      if(!raisedTickets.find((ticket) => ticket.name.get() === ticketInfo.name)) {
        console.log('Creating ticket ...');
        const ticketNode = await spinalServiceTicket.addTicket(
          ticketInfo,
          processNode.getId().get(),
          context.getId().get(),
          process.env.TMP_SPINAL_NODE_ID,
          'alarm'
        );
        
        console.log('Ticket created:', ticketNode);
        //add code de link ticket to the bim object
        const asset_name = notification.entity.asset.name;
        const AssetList = asset_name.match(/SACD-\w+/g);
        //console.log('Asset List:', AssetList);
        const bimObjects = await this.getBimObjects();
        //console.log('Bim Objects:', bimObjects.length);
        for (const asset of AssetList) {
        await Promise.all(bimObjects.map(async (bimObject) => {

          const RealBimObject = SpinalGraphService.getRealNode(bimObject.id.get());
          const attribut = await serviceDocumentation.findOneAttributeInCategory(RealBimObject,'Droople Attributs','LO_Nom_Référence');
          if(attribut!=-1){
           const deviceName = attribut.value.get();
           if (deviceName === asset) {
                     
            if(typeof ticketNode === 'string') {
            await SpinalGraphService.addChild(
              bimObject.id.get(),
              ticketNode,
              'SpinalSystemServiceTicketHasTicket',
              SPINAL_RELATION_PTR_LST_TYPE
            );
            console.log('Ticket linked to BIM object:', RealBimObject.getName().get());
            }
           }
          }
        }));}
      } else {
        console.log('Ticket already exists:', ticketInfo.name);
        continue;
      }
    }

    for(const ticket of raisedTickets){
   
      if(!notificationData.data.find((notification) => 
        notification.type === ticket.name.get().split('-')[0] 
      && notification.entity.asset.id === parseInt(ticket.name.get().split('-')[1]))) {
        console.log('Update step of ticket:', ticket.name.get());
        await spinalServiceTicket.moveTicketToNextStep(
          context.getId().get(),
          processNode.getId().get(),
          ticket.id.get(),
        );
      }
    }
    
  }


  dateToNumber(dateString: string | Date) {
    const dateObj = new Date(dateString);
    return dateObj.getTime();
  }

  async addAttributesToDevice(node: SpinalNode<any>,asset: IAsset, device: IDevice) {
      
      console.log('Adding attributes to device ...');
      await attributeService.addAttributeByCategoryName(node, 'Asset', 'Name', asset.name),
      await attributeService.addAttributeByCategoryName(node, 'Asset', 'Id', String(asset.id)),
      await attributeService.addAttributeByCategoryName(node, 'Asset', 'Type', asset.type),
      await attributeService.addAttributeByCategoryName(node, 'Asset', 'Space', asset.space),
      await attributeService.addAttributeByCategoryName(node, 'Device', 'Description', device.description),
      await attributeService.addAttributeByCategoryName(node, 'Device', 'Id', device.dev_id),
      await attributeService.addAttributeByCategoryName(node, 'Device', 'Sample rate', String(device.sample_rate)),
      await attributeService.addAttributeByCategoryName(node, 'Device', 'Sample rate extra', String(device.sample_rate_extra)),
      await attributeService.addAttributeByCategoryName(node, 'Device', 'Sample rate extra', String(device.min_rest_between_cycles))
    
      console.log('Attributes added to device ', device.dev_id);

  }

  async createEndpoint(
    deviceId: string,
    endpointData : ILastTelemetry
  ) {
    const context = await this.getNetworkContext();;
    const endpointNodeModel = new InputDataEndpoint(`${endpointData.data_type}-${endpointData.id}`, endpointData.value, endpointData.unit, InputDataEndpointDataType.Real, InputDataEndpointType.Other);

    const res = new SpinalBmsEndpoint(
      endpointNodeModel.name,
      endpointNodeModel.path,
      endpointNodeModel.currentValue,
      endpointNodeModel.unit,
      InputDataEndpointDataType[endpointNodeModel.dataType],
      InputDataEndpointType[endpointNodeModel.type],
      endpointNodeModel.id
    );
    const childId = SpinalGraphService.createNode(
      { type: SpinalBmsEndpoint.nodeTypeName, name: endpointNodeModel.name },
      res
    );
    await SpinalGraphService.addChildInContext(
      deviceId,
      childId,
      context.getId().get(),
      SpinalBmsEndpoint.relationName,
      SPINAL_RELATION_PTR_LST_TYPE
    );

    const node = SpinalGraphService.getRealNode(childId);
    //await this.addEndpointAttributes(node,measure);
    return node;
  }
  
  async createNotifEndpoint(
    deviceId: string,
    endpointName : string,
    endpointValue : string
  ) {
    const context = await this.getNetworkContext();;
    const endpointNodeModel = new InputDataEndpoint(endpointName,endpointValue,'', InputDataEndpointDataType.Real, InputDataEndpointType.Other);

    const res = new SpinalBmsEndpoint(
      endpointNodeModel.name,
      endpointNodeModel.path,
      endpointNodeModel.currentValue,
      endpointNodeModel.unit,
      InputDataEndpointDataType[endpointNodeModel.dataType],
      InputDataEndpointType[endpointNodeModel.type],
      endpointNodeModel.id
    );
    const childId = SpinalGraphService.createNode(
      { type: SpinalBmsEndpoint.nodeTypeName, name: endpointNodeModel.name },
      res
    );
    await SpinalGraphService.addChildInContext(
      deviceId,
      childId,
      context.getId().get(),
      SpinalBmsEndpoint.relationName,
      SPINAL_RELATION_PTR_LST_TYPE
    );

    const node = SpinalGraphService.getRealNode(childId);
    //await this.addEndpointAttributes(node,measure);
    return node;
  }
  
  async createDevice(deviceName) {
    const deviceNodeModel = new InputDataDevice(deviceName, 'device');
    //await this.nwService.updateData(deviceNodeModel);
    const device = await this.nwService.createNewBmsDevice(this.virtualNetworkContext.getId().get(),deviceNodeModel)
    console.log('Created device ', device.name.get());
    return device;
  }
  async updateAssetsData(assetData : IAsset[]){
    //const assetsToProcess = assetData.slice(0,50);
    //const assetsToProcess= assetData.filter((asset) => asset.name === 'Urinoir SACD-E0008');
    
    await Promise.all( assetData.map( async (asset) => {
      //for (const asset of assetsToProcess){
        console.log('Asset : ', asset.name);
        //console.log(asset.devices.length, ' devices found for asset ', asset.name);
        
        for(const device of asset.devices){

          let deviceNodes : SpinalNode<any>[] = await this.virtualNetworkContext.getChildren('hasBmsDevice');
          let deviceNode = deviceNodes.find((deviceNode) => deviceNode.getName().get() === asset.name);
          //console.log('Device Node : ', deviceNode);
           if (!deviceNode){
            
            const deviceInfo = await this.createDevice(asset.name);
            //const deviceInfo = await this.createDevice(asset.name);
            deviceNode = SpinalGraphService.getRealNode(deviceInfo.id.get());
            // deviceNode = await this.networkContext.findOneInContext(
            //   this.networkContext,
            //   (node) => node.getName().get() === device.dev_id
            // )
            await this.addAttributesToDevice(deviceNode, asset, device);
          
          }
          
          SpinalGraphService._addNode(deviceNode);
           // Add notifications as enppoints to the device 
          const notifications = device.notifications;
          //const notificationNames = Object.keys(notifications);
          //console.log(`Notifications for device`,notificationNames);
          const notif = await deviceNode.getChildren('hasBmsEndpoint');
          

           for (const key in notifications) {
            //console.log('Notification key : ', key);
              //if (notifications.hasOwnProperty(key)) {
                //console.log('Notification value : ', notifications[key]);
                
                let notifNode = notif.find((n) => n.getName().get() === key);
                let Value : any;
                if (!notifNode) {
                  console.log('Creating notification endpoint ...');
                  if (key.includes("last_checked")){
                     Value = String(new Date ((notifications[key])));
                  }else {
                     Value = String(notifications[key]);
                  }
                  
                  notifNode = await this.createNotifEndpoint(deviceNode.getId().get(),key,Value);
                  SpinalGraphService._addNode(notifNode);
                  await this.nwService.setEndpointValue(notifNode.info.id.get(), Value);
                  //await this.timeseriesService.pushFromEndpoint(
                   // notifNode.info.id.get(),
                    notifications[key]
                  //);
                  //const realNode = SpinalGraphService.getRealNode(
                    //notifNode.getId().get()
                  //);
                  //await attributeService.updateAttribute(
                  //  realNode,
                   // 'default',
                   // 'timeSeries maxDay',
                    //{ value: '366' }
                 // );
                
                }else{
                  SpinalGraphService._addNode(notifNode);
                  await this.nwService.setEndpointValue(
                    notifNode.info.id.get(),
                    Value
                  );
                  
                }
              //}
           }
          // Look for endpoints
  
          const endpoints = await deviceNode.getChildren('hasBmsEndpoint');
          const sensors = device.sensors;
          const sensorAddresses = sensors.map((sensor) => sensor.sensor_address);
          const filteredSensorAddresses = sensorAddresses.filter((address) => /^\d+$/.test(address));
          const sortedSensorAddresses = filteredSensorAddresses
            .map(Number)
            .sort((a, b) => a - b);
          
          const AssetName = asset.name
          const result = AssetName.match(/SACD-\w+/g);
          console.log('Asset Names : ', result);
          await Promise.all(device.last_telemetry.map( async (telemetry) => {
            if(telemetry.value === null) return;
            let endpointNode = endpoints.find((endpoint) => endpoint.getName().get() === `${telemetry.data_type}-${telemetry.id}`);
            let EndP_sensor_add=Number(telemetry.sensor_address);
            //console.log("ednpoint sensor address : ", EndP_sensor_add)
            
            if(!endpointNode){
              //create new endpoint
              
              endpointNode = await this.createEndpoint(deviceNode.getId().get(), telemetry);
              SpinalGraphService._addNode(endpointNode);
              await this.nwService.setEndpointValue(endpointNode.info.id.get(), telemetry.value);
              await this.timeseriesService.pushFromEndpoint(
                endpointNode.info.id.get(),
                telemetry.value
              );
              const realNode = SpinalGraphService.getRealNode(
                endpointNode.getId().get()
              );
               attributeService.updateAttribute(
                realNode,
                'default',
                'timeSeries maxDay',
                { value: '366' }
              );
              //ajouter l'attribut device_name
              if(result.length==2 && sortedSensorAddresses.length ==2){
                if(EndP_sensor_add == sortedSensorAddresses[0]){
                  await attributeService.addAttributeByCategoryName(realNode, 'Asset', 'device_name', result[0]);
                }
                else if(EndP_sensor_add == sortedSensorAddresses[1]){
                  await attributeService.addAttributeByCategoryName(realNode, 'Asset', 'device_name', result[1]);
                }
                else if(EndP_sensor_add==0){
                  await attributeService.addAttributeByCategoryName(realNode, 'Asset', 'device_name',result[0]+" + " +result[1]);
                }
                
              } else if(result.length==1 && sortedSensorAddresses.length ==0){
                await attributeService.addAttributeByCategoryName(realNode, 'Asset', 'device_name',result[0]);
              }
            }
            else {
              SpinalGraphService._addNode(endpointNode);
              await this.nwService.setEndpointValue(
                endpointNode.info.id.get(),
                telemetry.value
              );
              // await this.timeseriesService.pushFromEndpoint(
              //   endpointNode.info.id.get(),
              //   telemetry.value
              // );    
            }
          }))
  
  
  
          // for(const telemetry of device.last_telemetry){
          //   if(telemetry.value === null) continue;
  
          //   console.log('Matching telemetry... : ', telemetry.data_type, telemetry.id);
          //   let endpointNode = endpoints.find((endpoint) => endpoint.getName().get() === `${telemetry.data_type}-${telemetry.id}`);
            
          //   if(!endpointNode){
          //     //create new endpoint
          //     endpointNode = await this.createEndpoint(deviceNode.getId().get(), telemetry);
          //     SpinalGraphService._addNode(endpointNode);
          //     await this.nwService.setEndpointValue(endpointNode.info.id.get(), telemetry.value);
          //     await this.timeseriesService.pushFromEndpoint(
          //       endpointNode.info.id.get(),
          //       telemetry.value
          //     );
          //     const realNode = SpinalGraphService.getRealNode(
          //       endpointNode.getId().get()
          //     );
          //     await attributeService.updateAttribute(
          //       realNode,
          //       'default',
          //       'timeSeries maxDay',
          //       { value: '366' }
          //     );
          //   }
          //   else {
          //     SpinalGraphService._addNode(endpointNode);
          //     console.log('Updating endpoint value and timeseries ...');
          //     await this.nwService.setEndpointValue(
          //       endpointNode.info.id.get(),
          //       telemetry.value
          //     );
          //     await this.timeseriesService.pushFromEndpoint(
          //       endpointNode.info.id.get(),
          //       telemetry.value
          //     );   
          //     console.log('Endpoint value and timeseries updated');    
          //   }
          // }
  
  
  
          }
        
      
       }));
      
  } 

  async init(): Promise<void> {
    console.log('Initiating SyncRunPull');
    try {

      await this.initNetworkNodes();
      await this.pullAndUpdateTickets();

      console.log('Getting asset information...');
      const startTime = Date.now();
      const assetData = await getAssets();
      const assetFetchTime = (Date.now() - startTime) / 1000;
      console.log(`Assets received in ${assetFetchTime} seconds`);
      
      console.log('Updating data ...');
      const updateStartTime = Date.now();
      //await this.updateAssetsData(assetData.data);
      const updateTime = (Date.now() - updateStartTime) / 1000;
      console.log(`Data updated in ${updateTime} seconds`);
      
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
        await this.pullAndUpdateTickets();
        console.log('Getting asset information...');
        const assetData = await getAssets();
        const assetFetchTime = (Date.now() - before) / 1000;
        console.log(`Assets received in ${assetFetchTime} seconds`);
        console.log('Updating data ...');
        const updateStartTime = Date.now();
        await this.updateAssetsData(assetData.data);
        const updateTime = (Date.now() - updateStartTime) / 1000;
        console.log(`Data updated in ${updateTime} seconds`);
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
