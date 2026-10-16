use adx_agent_core::{probes::ProbeAction, TemplateVersion};

#[test]
fn jiuwen_template_waits_for_agentserver_port() {
    let template: TemplateVersion =
        serde_json::from_str(include_str!("../examples/jiuwen-sfs-template.json")).unwrap();
    template.validate().unwrap();
    assert_eq!(template.init_timeout_seconds, Some(30));
    assert!(matches!(
        template.readiness_probe.unwrap().action().unwrap(),
        ProbeAction::TcpSocket { port: 18092 }
    ));
}
