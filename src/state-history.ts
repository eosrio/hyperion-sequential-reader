import WebSocket from 'ws';

export class StateHistorySocket {
	private ws;
	private readonly shipUrl;
	private readonly max_payload_mb;
	retryOnDisconnect = true;
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
		console.log(`Connecting to ${this.shipUrl}...`);
		this.ws = new WebSocket(this.shipUrl, {
			perMessageDeflate: false,
			maxPayload: this.max_payload_mb * 1024 * 1024,
		});
		this.ws.on('open', () => {
			this.connected = true;
			console.log('Websocket connected!');
			if (onConnected) {
				onConnected();
			}
		});
		this.ws.on('message', (data) => onMessage(data));
		this.ws.on('close', () => {
			this.connected = false;
			console.log('Websocket disconnected!');
			if(this.retryOnDisconnect) {
				onDisconnect();
			}
		});
		this.ws.on('error', (err) => {
			console.log(`${this.shipUrl} :: ${err.message}`);
		});
	}

	close(graceful: boolean) {
		if(graceful) {
			this.retryOnDisconnect = false;
		}
		this.ws.close();
	}

	send(payload) {
		this.ws.send(payload);
	}
}