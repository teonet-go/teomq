// Import TeoProxyClient class and Command enum
import TeoProxyClient from "./teoproxy.js";
import { Command } from "./teoproxy.js";

// The broker Teonet peer name
const broker = "J4c0OciuN5R0cYfw652T9XkuvckAnUTJj5c";

// Create TeoProxy client object
let teo = new TeoProxyClient();

// Connect to Teonet proxy websocket and Teonet peer api.
let connect = () => teo.connect("fortune-gui.teonet.dev", broker, subscribe);
connect();

// On close reconnect after 1 second
teo.onclose = () => {
    document.getElementById("connection").innerHTML = "disconnected";
    document.getElementById("connection").classList.add('disconnected');
    document.getElementById("connection").classList.remove('connected');
    setTimeout(connect, "1000");
}

// On message received from Teonet peer
teo.onmessage = (pac) => {
    console.debug("onmessage", pac);

    // SendTo answer data set to dom element with id "fortune"
    if (pac.cmd == Command.SendTo) {
        document.getElementById("fortune").innerHTML = pac.data;
        return;
    }

    if (pac.cmd != 0) {
        return;
    }

    // Stream answer data set to dom elements with id "num_players" or "num_servers"
    let dataArray = pac.data.split("/");
    switch (dataArray[0]) {
        case "num_players":
            document.getElementById("num_players").innerHTML = dataArray[1];
            break;
        case "num_servers":
            document.getElementById("num_servers").innerHTML = dataArray[1];
            break;
    }
}

// Subscribe to broker when connected
function subscribe() {

    document.getElementById("connection").innerHTML = "connected";
    document.getElementById("connection").classList.add('connected');
    document.getElementById("connection").classList.remove('disconnected');

    teo.cmd.sendTo(broker, "msg", "Consumer");

    teo.cmd.stream(broker, "num_players");
    teo.cmd.stream(broker, "num_servers");

    teo.cmd.sendTo(broker, "msg", "subscribe/version");
    teo.cmd.sendTo(broker, "msg", "subscribe/num_players");
    teo.cmd.sendTo(broker, "msg", "subscribe/num_servers");
}

// Send next message request
let i = 0;
export function nextMessage() {
    teo.cmd.sendTo(broker, "hello", "Consumer " + i++ + "!");
}
