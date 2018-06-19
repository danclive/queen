#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Connect {
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub methods: Vec<String>
}

impl Connect {
    pub fn from_str(value: String) -> Option<Connect> {
        if !value.contains('&') {
            return None
        }

        let fragments = value.split('&').collect::<Vec<&str>>();

        let mut connect = Connect::default();

        for fragment in fragments.iter() {
            if !fragment.contains(':') {
                continue;
            }

            let key_value = fragment.split(':').collect::<Vec<&str>>();
            if key_value.len() != 2 {
                continue;
            }

            if key_value[0] == "username" {
                connect.username = key_value[1].to_owned();
            }

            if key_value[0] == "password" {
                connect.password = key_value[1].to_owned();
            }

            if key_value[0] == "methods" {
                if key_value[1].contains(',') {
                    connect.methods = key_value[1].split(',').map(|v| v.to_owned()).collect();
                } else {
                    connect.methods = vec![key_value[1].to_owned()];
                }
            }
        }

        Some(connect)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ConnectReply {
    pub id: u32,
    pub message: String
}
