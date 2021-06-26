package com.meituan.flinkdemo.Entity;

import com.alibaba.fastjson.JSON;
import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CommandLogRecord {
    private String ts;
    private LogType type;
    private String serialnumber;
    private String task_id;
    private String waybill_id;
    private CommandType command_type;
    private String message;
    private String dt;


    public static enum CommandType {
        ATT_REQ_TO_RESERVE_CUPBOARD("ATT_REQ_TO_RESERVE_CUPBOARD"),
        ATT_REQ_TO_OPEN_GATE("ATT_REQ_TO_OPEN_GATE"),
        ATT_REQ_TO_PLACE_CARGO("ATT_REQ_TO_PLACE_CARGO"),
        ATT_CARGO_ARRIVED_AIRPORT("ATT_CARGO_ARRIVED_AIRPORT"),
        ATT_REQ_TO_CLOSE_GATE("ATT_REQ_TO_CLOSE_GATE"),
        ATT_RES_TO_OPEN_CUPBOARD("ATT_RES_TO_OPEN_CUPBOARD"),
        ATT_FORCE_TO_OPEN_CUPBOARD("ATT_FORCE_TO_OPEN_CUPBOARD"),
        DTT_START_FLIGHT_TRAJECTORY("DTT_START_FLIGHT_TRAJECTORY"),
        DTT_UNMOUNT_BOX("DTT_UNMOUNT_BOX")
        ;
        private String type;
        CommandType(String type) {
            this.type = type;
        }
        public String getType() {
            return this.type;
        }
    }

    public static enum LogType {
        AIRPORT("airport"), DRONE("drone"),
        AIRPORT_ACK("airport_ack"), DRONE_ACK("drone_ack"),
        AIRPORT_RETRY("airport_retry"), DRONE_RETRY("drone_retry");
        private String type;
        LogType(String type) {
            this.type = type;
        }
        public String getType() {
            return this.type;
        }
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
