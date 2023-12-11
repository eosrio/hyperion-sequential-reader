import WebSocket from 'ws';

export class StateHistorySocket {
	private ws;
	private readonly shipUrl;
	private readonly max_payload_mb;

	connected = false;

	private onMessage;

	constructor(ship_url, max_payload_mb) {
		this.shipUrl = ship_url;
		if (max_payload_mb) {
			this.max_payload_mb = max_payload_mb;
		} else {
			this.max_payload_mb = 256;
		}
	}

	connect(onMessage, onDisconnect, onError, onConnected) {
		this.onMessage = function (msg) {
			if (!this.connected)
				return;

			return onMessage(msg)
		};
		this.ws = new WebSocket(this.shipUrl, {
			perMessageDeflate: false,
			maxPayload: this.max_payload_mb * 1024 * 1024,
		});
		this.ws.on('open', () => {
			this.connected = true;
			if (onConnected)
				onConnected();
		});
		this.ws.on('message', this.onMessage);
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

	close() {
		this.connected = false;
		this.ws.off('message', this.onMessage);
		this.ws.close();
	}

	send(payload) {
		this.ws.send(payload);
	}
}
