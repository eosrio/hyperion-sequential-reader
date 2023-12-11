import WebSocket from 'ws';
import console from "console";

export class StateHistorySocket {
	private ws;
	private readonly shipUrl;
	private readonly max_payload_mb;
	connected = false;

	constructor(ship_url, max_payload_mb) {
		this.shipUrl = ship_url;
		if (max_payload_mb) {
			this.max_payload_mb = max_payload_mb;
		} else {
			this.max_payload_mb = 256;
		}
	}

	connect(onMessage, onDisconnect, onError, onConnected) {
		this.ws = new WebSocket(this.shipUrl, {
			perMessageDeflate: false,
			maxPayload: this.max_payload_mb * 1024 * 1024,
		});
		this.ws.on('open', () => {
			this.connected = true;
			if (onConnected)
				onConnected();
		});
		this.ws.on('message', (data) => onMessage(data));
		this.ws.on('close', () => {
			this.connected = false;
            if (onDisconnect)
    			onDisconnect();
		});
		this.ws.on('error', (err) => {
            if (onError)
                onError(err);
		});
	}

	async close(): Promise<void> {
		this.ws.clients.forEach((client: WebSocket) => {
			if (client.readyState === WebSocket.OPEN) {
				client.close();
			}
		});
		return new Promise((resolve, reject) => {
			this.ws.close((error) => {
				if (error) {
					console.error('Error closing WebSocket server:', error);
					reject(error);
				} else {
					resolve();
				}
			});
		});
	}

	send(payload) {
		this.ws.send(payload);
	}
}
