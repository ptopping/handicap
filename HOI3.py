aircraft_tech = {
'single_engine_aircraft_design' : {
	'activate_unit' : 'interceptor' ,
	'activate_building' : 'air_base',
	'research_bonus_from' : {
		'aeronautic_engineering' : 1.0
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 0.5,
	#common for all techs.
	'start_year' : 1918,
	'folder' : 'fighter_folder'
},	

'twin_engine_aircraft_design' : {
	'activate_unit' : 'tactical_bomber' ,
	'research_bonus_from' : {
		'aeronautic_engineering' : 1.0
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 0.5,
	#common for all techs.
	'start_year' : 1918,
	'folder' : 'bomber_folder'
},	

'basic_aeroengine' : {
	'interceptor' : {
		'maximum_speed' : 50,
		'surface_defence' : 0.5,
		'air_defence' : 0.5,
		'range' : -25
	},
	'tactical_bomber' : {
		'maximum_speed' : 25,
		'air_defence' : 0.75,
		'range' : -25
	},
	'allow' : {
	 	'OR' : {
			'single_engine_aircraft_design' : 1,
			'twin_engine_aircraft_design' : 1
		}
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'single_engine_aircraft_practical' : 0.3,
		'twin_engine_aircraft_practical' : 0.4
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 2,
	#common for all techs.
	'start_year' : 1936,
	'folder' : 'fighter_folder'
}	,

'basic_small_fueltank' : {
	'interceptor' : {
		'range' : 75,
		'surface_defence' : -0.5,
		'air_defence' : -1
	},
	'allow' : {
		'single_engine_aircraft_design' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'single_engine_aircraft_practical' : 0.6,
		'fighter_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 1,
	#common for all techs.
	'start_year' : 1936,
	'folder' : 'fighter_folder'
},	

'basic_single_engine_airframe' : {
	'interceptor' : {
		'surface_defence' : 1,
		'air_defence' : 1
	},	
	'allow' : {
		'single_engine_aircraft_design' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'single_engine_aircraft_practical': 0.6,
		'fighter_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 1,
	#common for all techs.
	'start_year' : 1936,
	'folder' : 'fighter_folder'
},	

'basic_aircraft_machinegun' : {
	'interceptor' : {
		'air_attack' : 2.00
	}	,
	'tactical_bomber' : {
		'air_attack' : 0.5
	},
	'allow' : {
	 	'OR' : {
			'single_engine_aircraft_design' : 1,
			'twin_engine_aircraft_design' : 1
		}
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.25,
		'infantry_theory' : 0.05,
		'single_engine_aircraft_practical' : 0.3,
		'twin_engine_aircraft_practical' : 0.4
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 2,
	#common for all techs.
	'start_year' : 1936,
	'folder' : 'fighter_folder'
},	

'basic_medium_fueltank' : {
	'tactical_bomber' : {
		'range' : 75,
		'surface_defence' : -1,
		'air_defence' : -1
	},
	'allow' : {
		'twin_engine_aircraft_design' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'twin_engine_aircraft_practical' : 0.6,
		'tac_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 1,
	#common for all techs.
	'start_year' : 1936,
	'folder' : 'bomber_folder'
}	,

'basic_twin_engine_airframe' : {
	'tactical_bomber' : {
		'range' : 75,
		'surface_defence' : 2.5,
		'air_defence' : 1
	},
	'allow' : {
		'twin_engine_aircraft_design' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'twin_engine_aircraft_practical' : 0.6,
		'tac_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 1,
	#common for all techs.
	'start_year' : 1936,
	'folder' : 'bomber_folder'
},	

'basic_bomb' : {
	'tactical_bomber' : {
		'soft_attack' : 1,
		'hard_attack' : 0.5,
		'sea_attack' : 0.25,
		'strategic_attack' : 0.5
	},
	'allow' : {
		'twin_engine_aircraft_design' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'twin_engine_aircraft_practical' : 0.6,
		'tac_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 1,
	#common for all techs.
	'start_year' : 1936,
	'folder' : 'bomber_folder'
},	

'multi_role_fighter_development' : {
	'activate_unit' : 'multi_role',
	'allow' : {
		'basic_aeroengine' : 1,
		'basic_small_fueltank' : 1,
		'basic_single_engine_airframe' : 1,
		'basic_aircraft_machinegun' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'single_engine_aircraft_practical' : 0.6,
		'fighter_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 1,
	#common for all techs.
	'start_year' : 1938,
	'folder' : 'fighter_folder'
}	,

'cas_development' : {
	'activate_unit' : 'cas' ,
	'allow' : {
		'basic_aeroengine' : 1,
		'basic_small_fueltank' : 1,
		'basic_single_engine_airframe' : 1,
		'basic_aircraft_machinegun' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'single_engine_aircraft_practical' : 0.6,
		'cas_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 1,
	#common for all techs.
	'start_year' : 1937,
	'folder' : 'bomber_folder'
}	,

'nav_development' : {
	'activate_unit' : 'naval_bomber' ,
	'allow' : {
		'basic_aeroengine' : 1,
		'basic_medium_fueltank' : 1,
		'basic_twin_engine_airframe' : 1,
		'basic_aircraft_machinegun' : 1,
		'basic_bomb' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'twin_engine_aircraft_practical' : 0.6,
		'nav_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 1,
	#common for all techs.
	'start_year' : 1938,
	'folder' : 'bomber_folder'
},	

'basic_four_engine_airframe' : {
	'activate_unit' : 'transport_plane' ,
	'allow' : {
		'basic_twin_engine_airframe' : 1,
		'basic_aeroengine' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'four_engine_aircraft_practical' : 0.6,
		'strategic_air_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 1,
	#common for all techs.
	'start_year' : 1936,
	'folder' : 'bomber_folder'
},	

'basic_strategic_bomber' : {
	'activate_unit' : 'strategic_bomber' ,
	'allow' : {
		'basic_four_engine_airframe' : 1,
		'basic_medium_fueltank' : 1,
		'basic_aircraft_machinegun' : 1,
		'basic_bomb' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'four_engine_aircraft_practical' : 0.6,
		'strategic_air_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 1,
	#common for all techs.
	'start_year' : 1937,
	'folder' : 'bomber_folder'
}	,

'aeroengine' : {
	'interceptor' : {
		'maximum_speed' : 60,
		'range' : -25,
		'surface_defence' : 0.20,
		'air_defence' : 1.5
	},
	'multi_role' : {
		'maximum_speed' : 50,
		'range' : -50,
		'surface_defence' : 0.40,
		'air_defence' : 1.75
	},
	'tactical_bomber' : {
		'maximum_speed' : 25,
		'range' : -50,
		'surface_defence' : 0.75
	},
	'cas' : {
		'maximum_speed' : 25,
		'range' : -15,
		'surface_defence' : 1.5,
		'air_defence' : 1.15
	},
	'naval_bomber' : {
		'maximum_speed' : 25,
		'range' : -50,
		'surface_defence' : 0.75
	},
	'strategic_bomber' : {
		'maximum_speed' : 25,
		'range' : -50,
		'surface_defence' : 1.0
	},
	'cag' : {
		'maximum_speed' : 25,
		'range' : -15,
		'surface_defence' : 1.5,
		'air_defence' : 1.15
	},
	'allow' : {
	 	'OR' : {
			'basic_aeroengine' : 1
		}
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'single_engine_aircraft_practical' : 0.2,
		'twin_engine_aircraft_practical' : 0.2,
		'four_engine_aircraft_practical' : 0.3
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 7,
	#common for all techs.
	'start_year' : 1939,
	'first_offset' : 1941,
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'fighter_folder'
},	

'small_fueltank' : {
	'interceptor' : {
		'range' : 75,
		'surface_defence' : -0.15,
		'air_defence' : -1
	},
	'multi_role' : {
		'range' : 125,
		'surface_defence' : -0.30,
		'air_defence' : -1.125
	},
	'cas' : {
		'range' : 45,
		'surface_defence' : -0.625,
		'air_defence' : -0.8
	},
	'cag' : {
		'range' : 45,
		'surface_defence' : -0.625,
		'air_defence' : -0.8
	},
	'allow' : {
		'basic_small_fueltank' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'single_engine_aircraft_practical' : 0.6,
		'fighter_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 3,
	#common for all techs.
	'start_year' : 1939,
	'first_offset' : 1941,
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'fighter_folder'
}	,

'single_engine_airframe' : {
	'interceptor' : {
		'surface_defence' : 0.20,
		'air_defence' : 1.5
	},
	'multi_role' : {
		'surface_defence' : 0.40,
		'air_defence' : 1.75
	},
	'cas' : {
		'surface_defence' : 0.75,
		'air_defence' : 1.15
	},
	'cag' : {
		'surface_defence' : 0.75,
		'air_defence' : 1.15
	},
	'allow' : {
		'basic_single_engine_airframe' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'single_engine_aircraft_practical' : 0.6,
		'fighter_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 3,
	#common for all techs.
	'start_year' : 1939,
	'first_offset' : 1941,
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'fighter_folder'
},	

'single_engine_aircraft_armament' : {
	'interceptor' : {
		'air_attack' : 2.00,
		'soft_attack' : 0.25
	},
	'multi_role' : {
		'air_attack' : 1.75,
		'soft_attack' : 0.5
	},
	'cas' : {
		'soft_attack' : 0.25,
		'hard_attack' : 0.25
	},
	'cag' : {
		'air_attack' : 1.5,
		'soft_attack' : 0.25
	},
	'allow' : {
		'basic_aircraft_machinegun' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'single_engine_aircraft_practical' : 0.6,
		'fighter_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 3,
	#common for all techs.
	'start_year' : 1939,
	'first_offset' : 1941,
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'fighter_folder'
}	,

'medium_fueltank' : {
	'tactical_bomber' : {
		'range' : 125,
		'surface_defence' : -0.75,
		'air_defence' : -0.75
	},
	'naval_bomber' : {
		'range' : 125,
		'surface_defence' : -0.75,
		'air_defence' : -0.75
	},
	'allow' : {
		'basic_medium_fueltank' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'twin_engine_aircraft_practical' : 0.6,
		'tac_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 2,
	#common for all techs.
	'start_year' : 1939,
	'first_offset' : 1941,
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'bomber_folder'
}	,

'twin_engine_airframe' : {
	'tactical_bomber' : {
		'surface_defence' : 1.5,
		'air_defence' : 1.5
	},
	'naval_bomber' : {
		'surface_defence' : 1.5,
		'air_defence' : 1.5
	},
	'allow' : {
		'basic_twin_engine_airframe' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'twin_engine_aircraft_practical' : 0.6,
		'tac_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 2,
	#common for all techs.
	'start_year' : 1939,
	'first_offset' : 1941,
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'bomber_folder'
}	,

'air_launched_torpedo' : {
	'naval_bomber' : {
		'sea_attack' : 0.75,
		'sub_attack' : 0.75
	},
	'cag' : {
		'sea_attack' : 0.75,
		'sub_attack' : 0.75
	},
	'allow' : {
		'nav_development' : 1
	},
	'research_bonus_from' : {
		'submarine_engineering' : 0.15,
		'naval_engineering' : 0.15,
		'aeronautic_engineering' : 0.3,
		'twin_engine_aircraft_practical' : 0.4
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 1,
	#common for all techs.
	'start_year' : 1939,
	'first_offset' : 1941,
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'bomber_folder'
}	,

'small_bomb' : {
	'interceptor' : {
		'hard_attack' : 0.25,
		'soft_attack' : 0.25
	},
	'multi_role' : {
		'hard_attack' : 0.5,
		'soft_attack' : 0.5
	},
	'cas' : {
		'hard_attack' : 1.25,
		'soft_attack' : 0.25,
		'sea_attack' : 0.25
	},
	'cag' : {
		'hard_attack' : 0.25,
		'soft_attack' : 0.25,
		'sea_attack' : 0.25
	},
	'allow' : {
		'basic_bomb' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'single_engine_aircraft_practical' : 0.6,
		'cas_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 3,
	#common for all techs.
	'start_year' : 1939,
	'first_offset' : 1941,
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'bomber_folder'
},

'twin_engine_aircraft_armament' : {
	'naval_bomber' : {
		'air_defence' : 0.75
	}	,
	'tactical_bomber' : {
		'air_defence' : 0.75
	},
	'allow' : {
	 	'basic_aircraft_machinegun' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.25,
		'infantry_theory' : 0.05,
		'twin_engine_aircraft_practical' : 0.7
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 2,
	#common for all techs.
	'start_year' : 1939,
	'first_offset' : 1941,
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'bomber_folder'
},	

'medium_bomb' : {
	'naval_bomber' : {
		'hard_attack' : 0.25,
		'soft_attack' : 0.5,
		'sea_attack' : 0.25
	},
	'tactical_bomber' : {
		'soft_attack' : 1.5,
		'hard_attack' : 0.5,
		'sea_attack' : 0.25,
		'strategic_attack' : 0.5
	},
	'allow' : {
		'basic_bomb' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'twin_engine_aircraft_practical' : 0.6,
		'tac_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 2,
	#common for all techs.
	'start_year' : 1939,
	'first_offset' : 1941,
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'bomber_folder'
}	,

'large_fueltank' : {
	'transport_plane' : {
		'range' : 75,
		'surface_defence' : -1,
		'air_defence' : -0.75
	},
	'strategic_bomber' : {
		'range' : 150,
		'surface_defence' : -1,
		'air_defence' : -0.5
	},
	'allow' : {
		'basic_four_engine_airframe' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'four_engine_aircraft_practical' : 0.6,
		'strategic_air_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 2,
	#common for all techs.
	'start_year' : 1939,
	'first_offset' : 1941,
	'additional_offset' : 2,#one new every 2 years
	'max_level' : 12,
	'folder' : 'bomber_folder'
},

'four_engine_airframe' : {
	'transport_plane' : {
		'surface_defence' : 2.0,
		'air_defence' : 1.5
	},
	'strategic_bomber' : {
		'surface_defence' : 2.0,
		'air_defence' : 1.5
	},
	'allow' : {
		'basic_four_engine_airframe' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'four_engine_aircraft_practical' : 0.6,
		'strategic_air_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 2,
	#common for all techs.
	'start_year' : 1939,
	'first_offset' : 1941,
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'bomber_folder'
},		

'strategic_bomber_armament' : {
	'strategic_bomber' : {
		'air_defence' : 0.75
	},
	'allow' : {
		'basic_strategic_bomber' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.25,
		'infantry_theory' : 0.05,
		'four_engine_aircraft_practical' : 0.7
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 1,
	#common for all techs.
	'start_year' : 1939,
	'first_offset' : 1941,
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'bomber_folder'
}	,

'cargo_hold' : {
	'transport_plane' : {
		'transport_capability' : 1
	},
	'allow' : {
	 	'basic_four_engine_airframe' : 1
	},
	'research_bonus_from' : {
		'four_engine_aircraft_practical' : 0.7,
		'strategic_air_focus' : 0.3
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 1,
	#common for all techs.
	'start_year' : 1939,
	'first_offset' : 1941,
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'bomber_folder'
}	,

'large_bomb' : {
	'strategic_bomber': {
		'hard_attack' : 0.25,
		'soft_attack' : 0.5,
		'strategic_attack' : 2.00 
	},
	'allow' : {
		'basic_strategic_bomber' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'four_engine_aircraft_practical' : 0.6,
		'strategic_air_focus' : 0.1
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 1,
	#common for all techs.
	'start_year' : 1939,
	'first_offset' : 1941,
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'bomber_folder'
},

'advanced_aircraft_design' : {
	'research_bonus_from' : {
		'aeronautic_engineering' : 1.0
	},
	'allow' : {
		'radar' : 1,
		'OR' : {
			'single_engine_aircraft_design'  : 1,
			'twin_engine_aircraft_design' : 1
		}
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 2,
	#common for all techs.
	'start_year' : 1940,
	'folder' : 'fighter_folder'
},	
	
'small_airsearch_radar'  : {
	'interceptor' : {
		'air_detection' : 1.00,
		'night' : { 
			'attack' : 0.1
		}
	},
	'multi_role' : {
		'air_detection' : 1.00,
		'night' : { 
			'attack' : 0.1
		}
	},
	'cas' : {
		'air_detection' : 0.5,
		'night' : { 
			'attack' : 0.1
		}
	},
	'cag' : {
		'air_detection' : 1,
		'night' : { 
			'attack' : 0.1
		}
	},
	'research_bonus_from' : {
		'electornicegineering_theory' : 0.3,
		'electornicegineering_practical' : 0.7
	},
	'allow' : {
		'radar' : 2,
		'advanced_aircraft_design' : 1,
		'single_engine_aircraft_design'  : 1
	},
	'on_completion' : 'electornicegineering_practical',
	'difficulty' : 6,
	#common for all techs.
	'start_year' : 1940,
	'first_offset' : 1941,
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'fighter_folder'
}	,

'medium_airsearch_radar'  : {
	'naval_bomber' : {
		'air_detection' : 0.5,
		'night' : { 
			'attack' : 0.1
		}
	},
	'tactical_bomber' : {
		'air_detection' : 0.5,
		'night' : { 
			'attack' : 0.1
		}
	},
	'research_bonus_from' : {
		'electornicegineering_theory' : 0.3,
		'electornicegineering_practical' : 0.7
	},
	'allow' : {
		'radar' : 2,
		'advanced_aircraft_design' : 1,
		'twin_engine_aircraft_design'  : 1
	},
	'on_completion' : 'electornicegineering_practical',
	'difficulty' : 6,
	#common for all techs.
	'start_year' : 1940,
	'first_offset' : 1941,
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'bomber_folder'
}	,

'large_airsearch_radar'  : {
	'strategic_bomber' : {
		'air_detection' : 0.5,
		'night' : { 
			'attack' : 0.1
		}
	},
	'transport_plane' : {
		'air_detection' : 0.5,
		'night' : { 
			'attack' : 0.1
		}
	},
	'research_bonus_from' : {
		'electornicegineering_theory' : 0.3,
		'electornicegineering_practical' : 0.7
	},
	'allow' : {
		'radar' : 2,
		'advanced_aircraft_design' : 1,
		'basic_strategic_bomber' : 1
	},
	'on_completion' : 'electornicegineering_practical',
	'difficulty' : 6,
	#common for all techs.
	'start_year' : 1940,
	'first_offset' : 1941,
	'additional_offset' : 2,#one new every 2 years
	'max_level' : 12,
	'folder' : 'bomber_folder'
},	

'small_navagation_radar'  : {
	'interceptor' : {
		'surface_detection' : 0.5
	},
	'multi_role' : {
		'surface_detection' : 0.5
	},
	'cas' : {
		'surface_detection' : 0.5
	},
	'cag' : {
		'surface_detection' : 0.5
	},
	'research_bonus_from' : {
		'electornicegineering_theory' : 0.3,
		'electornicegineering_practical' : 0.7
	},
	'allow' : {
		'small_airsearch_radar' : 1
	},
	'on_completion' : 'electornicegineering_practical',
	'difficulty' : 6,
	#common for all techs.
	'start_year' : 1940,
	'first_offset' : 1941,
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'fighter_folder'
}	,

'medium_navagation_radar'  : {
	'naval_bomber' : {
		'surface_detection' : 6
	},
	'tactical_bomber' : {
		'surface_detection' : 3
	},
	'research_bonus_from' : {
		'electornicegineering_theory' : 0.3,
		'electornicegineering_practical' : 0.7
	},
	'allow' : {
		'medium_airsearch_radar' : 1
	},
	'on_completion' : 'electornicegineering_practical',
	'difficulty' : 6,
	#common for all techs.
	'start_year' : 1940,
	'first_offset' : 1941,
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'bomber_folder'
},	

'large_navagation_radar'  : {
	'strategic_bomber' : {
		'surface_detection' : 0.5
	},
	'research_bonus_from' : {
		'electornicegineering_theory' : 0.3,
		'electornicegineering_practical' : 0.7
	},
	'allow' : {
		'large_airsearch_radar' : 1
	},
	'on_completion' : 'electornicegineering_practical',
	'difficulty' : 6,
	#common for all techs.
	'start_year' : 1940,
	'first_offset' : 1941,
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'bomber_folder'
},	

'rocket_interceptor_tech' : {
	'activate_unit' : 'rocket_interceptor',
	'research_bonus_from' : {
		'rocket_science' : 0.3,
		'rocket_practical' : 0.7
	},
	'allow' : {
		'small_fueltank' : 1,
		'single_engine_airframe' : 3,
		'single_engine_aircraft_armament' : 2,
		'rocket_engine' : 1
	},
	'on_completion' : 'jetengine_theory',
	'difficulty' : 4,
	'start_year' : 1941,
	'folder' : 'fighter_folder'
},

'drop_tanks' : {
	'interceptor' : {
		'range' : 200
	},
	'multi_role' : {
		'range' : 200
	},
	'cas' : {
		'range' : 200
	},
	'cag' : {
		'range' : 150
	},
	'allow' : {
		'small_fueltank' : 3
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'single_engine_aircraft_practical' : 0.7
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 3,
	#common for all techs.
	'start_year' : 1943,
	'folder' : 'fighter_folder'
},	

'jet_engine' : {
	'interceptor' : {
		'maximum_speed' : 100,
		'range' : -50,
		'surface_defence' : 1,
		'air_defence' : 1
	},
	'multi_role' : {
		'maximum_speed' : 100,
		'range' : -50,
		'surface_defence' : 1,
		'air_defence' : 1
	},
	'tactical_bomber' : {
		'maximum_speed' : 50,
		'range' : -50,
		'air_defence' : 0.5
	},
	'cas' : {
		'maximum_speed' : 50,
		'range' : -50,
		'air_defence' : 0.5
	},
	'naval_bomber' : {
		'maximum_speed' : 50,
		'range' : -50,
		'air_defence' : 0.5
	},
	'strategic_bomber' : {
		'maximum_speed' : 50,
		'range' : -50,
		'air_defence' : 0.5
	},
	'cag' : {
		'maximum_speed' : 50,
		'range' : -25,
		'surface_defence' : 1,
		'air_defence' : 1
	},
	'allow' : {
	 	'aeroengine' : 1,
		'theorical_jet_engine' : 1
	},
	'research_bonus_from' : {
		'jetengine_theory' : 0.3,
		'jetengine_practical' : 0.7
	},
	'on_completion' : 'jetengine_theory',
	'difficulty' : 14,
	#common for all techs.
	'start_year' : 1943,
	'first_offset' : 1944,
	'additional_offset' : 3	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'fighter_folder'
}	
}

aircraft_doctrine = {
'fighter_pilot_training' : {
	'interceptor' : {
		'default_organisation' : 5
	},
	'multi_role' : {
		'default_organisation' : 5
	},
	'cag' : {
		'default_organisation' : 1 
	},
	'rocket_interceptor' : {
		'default_organisation' : 3 
	},
	'allow' : { 
		'single_engine_aircraft_design' : 1
	},
	'research_bonus_from' : {
		'fighter_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'fighter_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'fighter_groundcrew_training' : {
	'interceptor' : {
		'default_morale' : 0.05
	},
	'multi_role' : {
		'default_morale' : 0.05
	},
	'cag' : {		
		'default_morale' : 0.01
	},
	'rocket_interceptor' : {
		'default_morale' : 0.03 
	},
	'allow' : { 
		'single_engine_aircraft_design' : 1
	},
	'research_bonus_from' : {
		'fighter_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'fighter_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'interception_tactics' : {
	'air_intercept' : {
		'efficiency' : 0.05 #5% greater eff when on on intercept mission
	},
	'allow' : { 
		'single_engine_aircraft_design' : 1
	},
	'research_bonus_from' : {
		'fighter_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'fighter_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'fighter_ground_control' : {
	'radar_impact' : 0.05,
	'allow' : { 
		'single_engine_aircraft_design' : 1
	},
	'research_bonus_from' : {
		'fighter_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'fighter_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'bomber_targerting_focus' : {
	'bomber_targeting' : 0.05,
	'allow' : { 
		'single_engine_aircraft_design' : 1
	},
	'research_bonus_from' : {
		'fighter_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'fighter_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936	,#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'fighter_targerting_focus' : {
	'fighter_targeting' : 0.05,
	'allow' : { 
		'single_engine_aircraft_design' : 1
	},
	'research_bonus_from' : {
		'fighter_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'fighter_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'cas_pilot_training' : {
	'cas' : {
		'default_organisation' : 5
	},
	'cag' : {
		'default_organisation' : 2 
	},
	'allow' : { 
		'cas_development' : 1
	},
	'research_bonus_from' : {
		'cas_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'cas_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'cas_groundcrew_training' : {
	'cas' : {
		'default_morale' : 0.05
	},
	'cag' : {
		'default_morale' : 0.02 
	},
	'allow' : { 
		'cas_development' : 1
	},
	'research_bonus_from' : {
		'cas_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'cas_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'ground_attack_tactics' : {
	'ground_attack' : {
		'efficiency' : 0.10 #5% greater eff when on on ground attack missions
	},
	'allow' : { 
		'cas_development' : 1
	},
	'research_bonus_from' : {
		'cas_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'cas_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'forward_air_control' : {
	'frontline_focus' : 0.05,
	'allow' : { 
		'cas_development' : 1
	},
	'research_bonus_from' : {
		'cas_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'cas_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'battlefield_interdiction' : {
	'reserve_focus' : 0.05,
	'allow' : { 
		'cas_development' : 1
	},
	'research_bonus_from' : {
		'cas_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'cas_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'tac_pilot_training' : {
	'tactical_bomber' : {
		'default_organisation' : 5
	},
	'allow' : { 
		'twin_engine_aircraft_design' : 1
	},
	'research_bonus_from' : {
		'tac_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'tac_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'tac_groundcrew_training' : {
	'tactical_bomber' : {
		'default_morale' : 0.05
	},
	'allow' : { 
		'twin_engine_aircraft_design' : 1
	},
	'research_bonus_from' : {
		'tac_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'tac_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'interdiction_tactics' : {
	'interdiction' : {
		'efficiency' : 0.05 #5% greater eff when on on intercept mission
	},
	'allow' : { 
		'twin_engine_aircraft_design' : 1
	},
	'research_bonus_from' : {
		'tac_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no', 
	'on_completion' : 'tac_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'logistical_strike_tactics' : {
	'logistical_strike' : {
		'efficiency' : 0.05 #5% greater eff when on on logistical strike mission
	},
	'allow' : { 
		'twin_engine_aircraft_design' : 1
	},
	'research_bonus_from' : {
		'tac_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'tac_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'installation_strike_tactics' : {
	'installation_strike' : {
		'efficiency' : 0.05 #5% greater eff when on on Installation strike mission
	},
	'allow' : { 
		'twin_engine_aircraft_design' : 1
	},
	'research_bonus_from' : {
		'tac_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'tac_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'airbase_strike_tactics' : {
	'runway_cratering' : {
		'efficiency' : 0.05 #5% greater eff when on on 'runway_cratering' mission
	},
	'allow' : { 
		'twin_engine_aircraft_design' : 1
	},
	'research_bonus_from' : {
		'tac_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'tac_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'tactical_air_command' : {
	'ground_attack' : {
		'reduction_modifier' : -0.05 
	},
	'interdiction' : {
		'reduction_modifier' : -0.05 
	},
	'allow' : { 
		'twin_engine_aircraft_design' : 1
	},
	'research_bonus_from' : {
		'tac_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'tac_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'nav_pilot_training' : {
	'naval_bomber' : {
		'default_organisation' : 5
	},
	'cag' : {
		'default_organisation' : 2 
	},
	'allow' : { 
		'nav_development' : 1
	},
	'research_bonus_from' : {
		'nav_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'nav_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'nav_groundcrew_training' : {
	'naval_bomber' : {
		'default_morale' : 0.05
	},
	'cag' : {
		'default_morale' : 0.02 
	},
	'allow' : { 
		'nav_development' : 1
	},
	'research_bonus_from' : {
		'nav_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'nav_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'portstrike_tactics' : {
	'port_strike' : {
		'efficiency' : 0.05 #5% greater eff when on port strikes mission
	},
	'allow' : { 
		'nav_development' : 1
	},
	'research_bonus_from' : {
		'nav_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no', 
	'on_completion' : 'nav_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'navalstrike_tactics' : {
	'naval_strike' : {
		'efficiency' : 0.05 #5% greater eff when on naval strikes mission
	},
	'carrier_protection' : {
		'efficiency'  : 0.05 #5% greater eff when on 'cag' duty mission
	},
	'allow' : { 
		'nav_development' : 1
	},
	'research_bonus_from' : {
		'nav_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'nav_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'naval_air_targeting' : {
	'naval_air_target_chance' : 0.05,
	'allow' : { 
		'nav_development' : 1
	},
	'research_bonus_from' : {
		'nav_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'nav_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'naval_tactics' : {
	'naval_strike' : {
		'reduction_modifier' : -0.05 
	},
	'port_strike' : {
		'reduction_modifier' : -0.05 
	},
	'allow' : { 
		'nav_development' : 1
	},
	'research_bonus_from' : {
		'nav_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'nav_focus',
	'difficulty' : 5,
	#common for all techs,.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'heavy_bomber_pilot_training' : {
	'strategic_bomber' : {
		'default_organisation' : 5
	},
	'transport_plane' : {
		'default_organisation' : 5
	},
	'allow' : { 
		'basic_four_engine_airframe' : 1
	},
	'research_bonus_from' : {
		'strategic_air_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'strategic_air_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'heavy_bomber_groundcrew_training' : {
	'strategic_bomber' : {
		'default_morale' : 0.05
	},
	'transport_plane' : {
		'default_morale' : 0.05
	},
	'allow' : { 
		'basic_four_engine_airframe' : 1
	},
	'research_bonus_from' : {
		'strategic_air_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'strategic_air_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'strategic_bombardment_tactics' : {
	'strategic_bomb' : {
		'efficiency' : 0.05 #5% greater eff when on on 'strategic_bomb' mission
	},
	'allow' : { 
		'basic_strategic_bomber' : 1
	},
	'research_bonus_from' : {
		'strategic_air_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'strategic_air_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'airborne_assault_tactics' : {
	'paradrop_mission' : {
		'efficiency' : 0.05	#5% efficinency increase airborne assault missions
	},
	'allow' : { 
		'basic_four_engine_airframe' : 1,
		'paratrooper_infantry' : 1
	},
	'research_bonus_from' : {
		'strategic_air_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'strategic_air_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
},

'strategic_air_command' : {
	'strategic_bomb' : {
		'reduction_modifier' : -0.05 
	},
	'allow' : { 
		'basic_strategic_bomber' : 1
	},
	'research_bonus_from' : {
		'strategic_air_focus' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'change' : 'no' ,
	'on_completion' : 'strategic_air_focus',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'air_doctrine_folder'
}
}
armor_tech = {

'lighttank_brigade' : {
	'activate_unit' : 'light_armor_brigade' ,
	'research_bonus_from' : {
		'automotive_theory' : 1.0
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 3,
	#common for all techs.
	'start_year' : 1918,
	'folder' : 'armour_folder'
},	

'lighttank_gun' : {
	'light_armor_brigade' : {
		'soft_attack' : 0.5,
		'hard_attack' : 0.5,
		'ap_attack' : 1,
		'toughness'  : -0.3,
		'maximum_speed' : -0.1,
	},
        'mechanized_brigade' : {
            'soft_attack' : 0.3,
	    'hard_attack' : 0.2,
	    'toughness'  : -0.1,
	    'maximum_speed' : -0.1
        },
	'allow' : {
		'lighttank_brigade' : 1
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'artillery_practical' : 0.6,
		'human_wave_theory' : 0.1
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 3,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'lighttank_engine' : {
	'light_armor_brigade' : {
		'maximum_speed' : 1.00,
		'toughness'  : -0.3
	},
    'mechanized_brigade' : {
        'maximum_speed' : 0.5,
        'toughness'  : -0.1
    },
    'motorized_brigade' : {
        'maximum_speed' : 0.2
    },
    'armored_car_brigade' : {
        'maximum_speed' : 0.2,
        'toughness' : -0.4
    },
	'waffen_brigade' : {
        'maximum_speed' : 0.2
    },
	'allow' : {
		'lighttank_brigade' : 1
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'armour_practical' : 0.6,
		'mechanicalengineering_theory' : 0.1
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 3,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'lighttank_armour' : {
	'activate_unit' : 'tank_destroyer_brigade',
	'light_armor_brigade' : {
		'maximum_speed' : -0.40,
		'defensiveness'  : 0.6,
        'toughness' : 0.3,
		'armor_value' : 1
	},
    'mechanized_brigade' : {
        'softness'  : -0.01,
        'maximum_speed' : -0.2,
        'defensiveness'  : 0.1,
        'toughness' : 0.1
    },
    'motorized_brigade' : {
        'softness'  : -0.01,
        'maximum_speed' : -0.1
    },
	'waffen_brigade' : {
        'softness'  : -0.01,
        'maximum_speed' : -0.1
    },
	'allow' : {
		'lighttank_brigade' : 1
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'armour_practical' : 0.6,
		'grand_battleplan_theory' : 0.1
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 3,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'lighttank_reliability' : {
	'light_armor_brigade' : {
	    'toughness' : 0.9
	},
	'mechanized_brigade' : {
            'toughness' : 0.3
        },
        'armored_car_brigade' : {
            'toughness' : 0.8
        },
	'allow' : {
		'lighttank_brigade' : 1
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'armour_practical' : 0.6,
		'spearhead_theory' : 0.1
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 3,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'tank_brigade' : {
	'activate_unit' : 'armor_brigade' ,
	'allow' : {
		'lighttank_gun' : 2,
		'lighttank_engine' : 2,
		'lighttank_armour' : 2,
		'lighttank_reliability' : 2
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'armour_practical' : 0.7
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1936,
	'folder' : 'armour_folder'
}	,

'tank_gun' : {
	'armor_brigade' : {
		'soft_attack' : 1.0,
		'hard_attack' : 1.0,
		'ap_attack' : 1,
		'toughness'  : -0.5,
		'maximum_speed' : -0.25
	},
	'allow' : {
		'tank_brigade' : 1
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'artillery_practical' : 0.6,
		'armour_practical' : 0.1
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1936,
	'first_offset' : 1938,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'tank_engine' : {
	'armor_brigade' : {
		'maximum_speed' : 1.00,
		'toughness'  : -0.5
	},
	'sp_artillery_brigade' : {
		'maximum_speed' : 1.00,
		'toughness'  : -0.3
	},
	'sp_rct_artillery_brigade' : {
		'maximum_speed' : 1.00,
		'toughness'  : -0.3
	},
	'tank_destroyer_brigade' : {
		'maximum_speed' : 1.00,
		'toughness'  : -0.3
	},
	'allow' : {
		'tank_brigade' : 1
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'armour_practical' : 0.6,
		'mechanicalengineering_theory' : 0.1
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1936,
	'first_offset' : 1938,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'tank_armour' : {
	'armor_brigade' : {
		'maximum_speed' : -0.25,
		'defensiveness'  : 1,
		'toughness' : 0.5,
		'armor_value' : 1
	},
	'sp_artillery_brigade' : {
		'maximum_speed' : -0.50,
		'defensiveness'  : 0.6,
		'toughness' : 0.2
	},
	'sp_rct_artillery_brigade' : {
		'maximum_speed' : -0.50,
		'defensiveness'  : 0.6,
		'toughness' : 0.2
	},
	'tank_destroyer_brigade' : {
		'maximum_speed' : -0.50,
		'defensiveness'  : 0.6,
		'toughness' : 0.2
	},
	'allow' : {
		'tank_brigade' : 1
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'armour_practical' : 0.6,
		'spearhead_theory' : 0.1
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1936,
	'first_offset' : 1938,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'tank_reliability' : {
	'armor_brigade' : {
		'toughness' : 1.5
	},
	'sp_artillery_brigade' : {
		'toughness' : 0.4
	},
	'sp_rct_artillery_brigade' : {
		'toughness' : 0.4
	},
	'tank_destroyer_brigade' : {
		'toughness' : 0.4
	},
	'allow' : {
		'tank_brigade' : 1
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'armour_practical' : 0.6,
		'spearhead_theory': 0.1
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1936,
	'first_offset' : 1938,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'heavy_tank_brigade' : {
	'activate_unit' : 'heavy_armor_brigade',
	'allow' : {
		'tank_gun' : 2,
		'tank_engine' : 2,
		'tank_armour' : 2,
		'tank_reliability' : 2
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'armour_practical' : 0.7
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 7,
	#common for all techs.
	'start_year' : 1938,
	'folder' : 'armour_folder'
}	,

'heavy_tank_gun' : {
	'heavy_armor_brigade' : {
		'soft_attack' : 1,
		'hard_attack' : 1.25,
		'ap_attack' : 1,
		'toughness'  : -0.3,
		'maximum_speed' : -0.25
	},
	'allow' : {
		'heavy_tank_brigade' : 1
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'artillery_practical' : 0.6,
		'grand_battleplan_theory' : 0.1
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 7,
	#common for all techs.
	'start_year' : 1938,
	'first_offset' : 1939,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'heavy_tank_engine' : {
	'heavy_armor_brigade' : {
		'maximum_speed' : 0.7,
		'toughness'  : -0.3
	},
	'allow' : {
		'heavy_tank_brigade' : 1
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'armour_practical' : 0.6,
		'mechanicalengineering_theory' : 0.1
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 7,
	#common for all techs.
	'start_year' : 1938,
	'first_offset' : 1939,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'heavy_tank_armour' : {
	'heavy_armor_brigade' : {
		'maximum_speed' : -0.25,
		'defensiveness'  : 1.5,
		'toughness' : 0.3,
		'armor_value' : 1
	},
	'tank_destroyer_brigade' : {
		'maximum_speed' : -0.25,
		'softness' : -0.01,
		'defensiveness'  : 0.3,
		'armor_value' : 1
	},
	'allow' : {
		'heavy_tank_brigade' : 1
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'armour_practical' : 0.6,
		'grand_battleplan_theory' : 0.1
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 7,
	#common for all techs.
	'start_year' : 1938,
	'first_offset' : 1939,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'heavy_tank_reliability' : {
	'heavy_armor_brigade' : {
		'toughness' : 0.9
	},
	'allow' : {
		'heavy_tank_brigade' : 1
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'armour_practical' : 0.6,
		'human_wave_theory' : 0.1
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 7,
	#common for all techs.
	'start_year' : 1938,
	'first_offset' : 1939,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'armored_car_armour' : {
	'armored_car_brigade' : {
            'maximum_speed' : -0.1,
            'defensiveness'  : 0.8,
            'toughness' : 0.4,
			'armor_value' : 0.75
	},
	'allow' : {
		'lighttank_brigade' : 1
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'mobile_practical' : 0.6,
		'spearhead_theory' : 0.1
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 3,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'armored_car_gun' : {
	'armored_car_brigade' : {
		'soft_attack' : 0.4,
		'ap_attack' : 0.75
	},
	'allow' : {
		'lighttank_brigade' : 1
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'mobile_practical' : 0.6,
		'spearhead_theory' : 0.1 
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 3,
	#common for all techs.
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'SP_brigade' : {
	'activate_unit' : 'sp_artillery_brigade' ,
	'allow' : {
		'lighttank_gun' : 3,
		'lighttank_engine' : 3,
		'lighttank_armour' : 3,
		'lighttank_reliability' : 3
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'armour_practical' : 0.6,
		'superior_firepower_theory' : 0.1
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1938,
	'folder' : 'armour_folder'
},	

'mechanised_infantry' : {
	'activate_unit' : 'mechanized_brigade' ,
	'allow' : {
		'mortorised_infantry' : 1,
		'tank_brigade' : 1,
		'smallarms_technology' : 3,
		'infantry_support' : 3,
		'infantry_guns' : 3,
		'infantry_at' : 3,
	},
	'research_bonus_from' : {
		'mobile_theory' : 0.3,
		'mobile_practical' : 0.6,
		'superior_firepower_theory' : 0.1
	},
	'on_completion' : 'mobile_theory',
	'difficulty' : 5,
	'start_year' : 1941,
	'folder' : 'infantry_folder'
},

'super_heavy_tank_brigade' : {
	'activate_unit' : 'super_heavy_armor_brigade',
	'allow' : {
		'heavy_tank_gun' : 2,
		'heavy_tank_engine' : 2,
		'heavy_tank_armour' : 2,
		'heavy_tank_reliability' : 2
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'armour_practical' : 0.7
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 8,
	#common for all techs.
	'start_year' : 1943,
	'folder' : 'armour_folder'
},

'super_heavy_tank_gun' : {
	'super_heavy_armor_brigade' : {
		'soft_attack' : 1,
		'hard_attack' : 1.25,
		'toughness'  : -0.25,
		'maximum_speed' : -0.25,
		'ap_attack' : 1
	},
	'allow' : {
		'super_heavy_tank_brigade' : 1
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'artillery_practical' : 0.6,
		'spearhead_theory' : 0.1
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 8,
	#common for all techs.
	'start_year' : 1943,
	'first_offset' : 1944,	#2nd model is from 1944
	'additional_offset' : 3	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'super_heavy_tank_engine' : {
	'super_heavy_armor_brigade' : {
		'maximum_speed' : 0.7,
		'toughness'  : -0.25
	},
	'allow' : {
		'super_heavy_tank_brigade' : 1
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'armour_practical' : 0.6,
		'mechanicalengineering_theory' : 0.1
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 8,
	#common for all techs.
	'start_year' : 1943,
	'first_offset' : 1944,	#2nd model is from 1944
	'additional_offset' : 3,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'super_heavy_tank_armour' : {
	'super_heavy_armor_brigade' : {
		'maximum_speed' : -0.25,
		'defensiveness'  : 1.5,
		'toughness' : 0.25,
		'armor_value' : 0.5
	},
	'allow' : {
		'super_heavy_tank_brigade' : 1
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'armour_practical' : 0.6,
		'spearhead_theory' : 0.1
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 8,
	#common for all techs.
	'start_year' : 1943,
	'first_offset' : 1944,	#2nd model is from 1944
	'additional_offset' : 3	,#one new every 2 years
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'super_heavy_tank_reliability' : {
	'super_heavy_armor_brigade' : {
		'toughness' : 0.75
	},
	'allow' : {
		'super_heavy_tank_brigade' : 1
	},
	'research_bonus_from' : {
		'automotive_theory' : 0.3,
		'armour_practical' : 0.6,
		'spearhead_theory' : 0.1
	},
	'on_completion' : 'automotive_theory',
	'difficulty' : 8,
	#common for all techs.
	'start_year' : 1943,
	'first_offset' : 1944,	#2nd model is from 1944
	'additional_offset' : 3,	#one new every 2 years
	'max_level' : 12,
	'folder' : 'armour_folder'
}
}
arty_tech = {

'art_barrell_ammo' : {
	'artillery_brigade' : {
		'soft_attack' : 0.6
	},
	'sp_artillery_brigade' : {
		'soft_attack' : 0.6
	},
	'allow' : {
		'infantry_activation' : 1
	},
	'research_bonus_from' : {
		'artillery_theory' : 0.3,
		'artillery_practical' : 0.6,
		'superior_firepower_theory' : .1
	},
	'on_completion' : 'artillery_theory',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1932,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'art_carriage_sights' : {
	'artillery_brigade' : {
		'defensiveness' : 0.2,
		'toughness' : 0.1,
		'hard_attack' : 0.1
	},
	'sp_artillery_brigade' : {
		'hard_attack' : 0.1,
		'defensiveness' : 0.1,
		'toughness' : 0.1
	},
	'allow' : {
		'infantry_activation' : 1
	},
	'research_bonus_from' : {
		'artillery_theory' : 0.3,
		'artillery_practical' : 0.6,
		'human_wave_theory' : 0.1
	},
	'on_completion' : 'artillery_theory',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1932,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'at_barrell_sights' : {
    'anti_tank_brigade' : {
        'defensiveness' : 0.2,
        'hard_attack' : 0.2
    },
    'tank_destroyer_brigade' : {
        'hard_attack' : 0.5
    },
    'allow' : {
        'infantry_activation' : 1
    },
    'research_bonus_from' : {
        'artillery_theory' : 0.3,
        'artillery_practical' : 0.6,
        'spearhead_theory' : 0.1
    },
    'on_completion' : 'artillery_theory',
    'difficulty' : 1,
    'start_year' : 1918,
    'first_offset' : 1936,
    'additional_offset' : 2,
    'max_level' : 12,
    'folder' : 'armour_folder'
},

'at_ammo_muzzel' : {
    'anti_tank_brigade' : {
        'hard_attack' : 0.8,
		'ap_attack' : 1
    },
    'tank_destroyer_brigade' : {
        'hard_attack' : 0.8,
		'ap_attack' : 1
    },
    'allow' : {
        'infantry_activation' : 1
    },
    'research_bonus_from' : {
        'artillery_theory' : 0.3,
        'artillery_practical' : 0.6,
        'grand_battleplan_theory' : 0.1
    },
    'on_completion' : 'artillery_theory',
    'difficulty' : 1,
    'start_year' : 1918,
    'first_offset' : 1936,
    'additional_offset' : 2,
    'max_level' : 12,
    'folder' : 'armour_folder'
},

'aa_barrell_ammo' : {
	'anti_air_brigade' : {
		'air_attack' : 0.3,
		'air_defence' : 0.3
	},
	'mot_aa_brigade' : {
		'air_attack' : 0.3,
		'air_defence' : 0.3
	},
	'allow' : {
		'infantry_activation' : 1
	},
	'research_bonus_from' : {
		'artillery_theory' : 0.3,
		'artillery_practical' : 0.6,
		'aeronautic_engineering' : 0.1
	},
	'on_completion' : 'artillery_theory',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1936,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'aa_carriage_sights' : {
	'anti_air_brigade' : {
		'hard_attack' : 0.3,
		'soft_attack' : 0.3,
		'defensiveness' : 0.2
	},
	'mot_aa_brigade' : {
		'hard_attack' : 0.3,
		'soft_attack' : 0.3,
		'defensiveness' : 0.2
	},
	'allow' : {
		'infantry_activation' : 1
	},
	'research_bonus_from' : {
		'artillery_theory' : 0.3,
		'artillery_practical' : 0.6,
		'air_doctrine_practical' : 0.1
	},
	'on_completion' : 'artillery_theory',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1936,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'rocket_art' : {
	'activate_unit' : 'rocket_artillery_brigade' ,
	'allow' : {
		'infantry_activation' : 1,
		'art_carriage_sights' : 2
	},
	'research_bonus_from' : {
		'rocket_science' : 0.3,
		'artillery_practical' : 0.7
	},
	'on_completion' : 'rocket_science',
	'difficulty' : 3,
	'start_year' : 1939,
	'folder' : 'armour_folder'
},
	
'rocket_art_ammo' : {
	'rocket_artillery_brigade' : {
		'soft_attack' : 1
	},
	'sp_rct_artillery_brigade' : {
		'soft_attack' : 1
	},
	'allow' : {
		'rocket_art' : 1
	},
	'research_bonus_from' : {
		'rocket_science' : 0.3,
		'artillery_practical' : 0.7
	},
	'on_completion' : 'rocket_science',
	'difficulty' : 1,
	'start_year' : 1939,
	'first_offset' : 1940,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'armour_folder'
},

'rocket_carriage_sights' : {
	'rocket_artillery_brigade' : {
		'defensiveness' : 0.2,
		'toughness' : 0.2
	},
	'sp_rct_artillery_brigade' : {
		'toughness' : 0.2
	},
	'allow' : {
		'rocket_art' : 1
	},
	'research_bonus_from' : {
		'rocket_science' : 0.3,
		'artillery_practical' : 0.7
	},
	'on_completion' : 'rocket_science',
	'difficulty' : 1,
	'start_year' : 1939,
	'first_offset' : 1940,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'armour_folder'
}
}
base_values = {
	'war_consumer_goods_demand' : 0.15,
	'peace_consumer_goods_demand' : 0.25,
	'global_manpower' : 4,
	'global_leadership' : 3.5,
	'ic' : 5
}
buildings = {
'air_base' : {
	'on_completion' : 'construction_practical',
	'completion_size' : 0.13,
	'air_capacity' : 1,
	'capital' : 'yes',
	'onmap' : 'yes',
	'cost' : 4,
	'time' : 90,
	'max_level' : 10,
	'visibility' : 'yes'
},

'naval_base' : {
	'on_completion' : 'construction_practical',
	'completion_size' : 0.33,
	'naval_capacity' : 1,
	'capital' : 'yes',
	'onmap' : 'yes',
	'port' : 'yes',
	'cost' : 5,
	'time' : 180,
	'max_level' : 10,
	'visibility' : 'yes'
},

'coastal_fort' : {
	'on_completion' : 'construction_practical',
	'completion_size' : 0.33,
	'coastal_fort_level' : 1,
	'onmap' : 'yes',
	'visibility' : 'yes',
	'port' : 'yes',
	'orientation' : 'yes',
	'cost' : 5,
	'time' : 180,
	'max_level' : 10
},

'land_fort' : {
	'on_completion' : 'construction_practical',
	'completion_size' : 0.33,
	'fort_level' : 1,
	'onmap' : 'yes',
	'visibility' : 'yes',
	'cost' : 5,
	'time' : 180,
	'max_level' : 10
},

'anti_air' : {
	'completion_size' : 0.25,
	'on_completion' : 'artillery_practical',
	'local_anti_air' : 1,
	'capital' : 'yes',
	'onmap' : 'yes',
	'visibility' : 'yes',
	'cost' : 3,
	'time' : 60,
	'max_level' : 10,
	'damage_factor' : 0.25
},

'radar_station' : {
	'on_completion' : 'electornicegineering_practical',
	'completion_size' : 0.5,
	'radar_level' : 1,
	'capital' : 'yes',
	'onmap' : 'yes',
	'visibility' : 'yes',
	'cost' : 2,
	'time' : 180,
	'max_level' : 10
},

'underground' : {
	'onmap' : 'yes',
	'capital' : 'yes',
	'local_underground' : 1,
	'visibility' : 'yes',
	'cost' : 4,
	'time' : 180,
	'on_completion' : 'militia_theory',
	'completion_size' : 0.1,
	'max_level' : 1,
	'show_for_province' : 'no',
	'repair' : 'no'
},

'nuclear_reactor' : {
	'on_completion' : 'nuclear_bomb',
	'completion_size' : 6,
	'capital' : 'yes',
	'onmap' : 'yes',
	'visibility' : 'yes',
	'cost' : 50,
	'time' : 180,
	'max_level' : 10
},

'rocket_test' : {
	'on_completion' : 'rocket_practical',
	'completion_size' : 6,
	'capital' : 'yes',
	'onmap' : 'yes',
	'visibility' : 'yes',
	'cost' : 20,
	'time' : 180,
	'max_level' : 10
},

'industry' : {
	'on_completion' : 'construction_practical',
	'completion_size' : 0.67,
	'ic' : 1,
	'capital' : 'yes',
	'onmap' : 'no',
	'cost' : 5,
	'time' : 365,
	'max_level' : 10
},

'infra' : {
	'on_completion' : 'construction_practical',
	'completion_size' : 0.13,
	'infrastructure' : 0.1,
	'onmap' : 'no',
	'cost' : 0.5,
	'time' : 180,
	'max_level' : 10,
	'damage_factor' : 0.25
}
}
combined_arms = {
	'infantry_unit_type' : {
		'base' : 'yes',
		'value' : 0
	},

	'armor_unit_type' : {
		'value' : 0.05
	},

	'artillery_unit_type' : {
		'value' : 0.05
	},

	'direct_fire_unit_type' : {
		'value' : 0.05
	},

	'support_unit_type' : {
		'value' : 0.05
	}
}
defines = {
'start_date' : '1936.1.1',
'end_date' : '1948.1.1',
'land_combat' : 'land_doctrine_practical',
'naval_combat' : 'naval_doctrine_practical',
'air_combat' : 'air_doctrine_practical',
'bombing' : 'air_doctrine_practical',
'base_militia' : 'militia_brigade'
}
country = {
	'CORE_LOSE' 				: 50,
	'CORE_GAIN' 				: 50,
	'YEARS_OF_NATIONALISM' 	: 20,   # Years of Nationalism
	'YEARS_UNTIL_BROKEN' 		: 2,    # Years until rebel held capital results in broken country.
	'REBEL_ACCEPTANCE_MONTHS' : 60,
	'ELECTION_RANDOMNESS' 	: 0.15,
	'ELECTION_MINISTER_DISSENT_PENALTY' : -0.1,
	'MAX_NUMBER_OF_SPIES' 	: 10,
	'SPY_DETECTION_CHANCE' 	: 0.1,
	'SPY_MISSION_CHANGE_DAYS' : 7,
	'SPY_CONNECTION_DETECTION_CHANCE' 	 : 0.5,
	'SPY_BASE_CONNECTION_DETECTION_CHANCE' : 0.5,
	'SPY_UNIT_DETECTION_CHANCE' 		: 0.5,
	'SPY_INTEL_MISSION_BONUS' 		: 0.5,   # % better when focusing on a field
	'SPY_PARTY_ORGANIZATION_BOOST' 	: 0.01,
	'BASE_TECH_DECAY' 				: -0.025,  # how many percent lost each month in tech categories.
	'MAX_TECH_ABILITY'				: 99,
	'TECH_ABILITY_GAIN_DIVISOR'		: 10,
	'NUKE_UNITY_IMPACT'	: 1.0,		# impact from being nukes.
	'MOBILIZE_THREAT_IMPACT' : 2.5,	# threat from when mobilizing.
	'SPY_THREAT_INCREASE_ACTION' : 0.07,	# daily threat increase from spy action
	'POPULARITIY_RANDOMNESS' : 10,
	'PARTY_POP_CHANGE_RATE' : 10, # Inversly affects how fast party popularity gets closer to organisation
	'MOBILIZATION_BONUS_DAYS' : 30, # Number of days reinforcements benefit from mobilization
	'NO_MISSION_SPY_DETECTION_MULTIPLIER' : 0.1, # The risk of being detected is multiplied with this number when not performing any missions
	'DEMOBILIZATION_FACTOR' : 0.5, # The amount of manpower you get back from units when demobilizing.
	'NUMBER_OF_VICTORY_CONDITIONS' : 15, # The number of victory conditions each faction needs to pick.
	'VICTORY_CONDITIONS_TO_WIN' : 12, # The number of victory conditions each faction needs to win.
	'PARTY_POPULARITY_CUTOFF' : 0.1, # limit when calculating cabinet positions
	'COUP_CHANCE_PER_POSITION' : 0.2,
	'PEACETIME_MANPOWER_ROTATION_LOW_SUPPLY_FACTOR' : 1.25, # affects peacetime_manpower_rotation modifier due to low supplies.
	'SPY_BASE_PRODUCTION_DETECTION_CHANCE' : 0.5,
	'SPY_COVERT_OPS_GAIN' : 0.5, # monthly gain per spy
	'SPY_COVERT_OPS_MAX' : 50,
	'COUP_COVERT_OPS_COST' : 20,
	'ROUGH_COUNTRY_RUIN_EST_MODIFIER' : 0.15, # balance rough estimation of country in ruins (bombing damage to % estimation used in strat warfare window)
	'TECHNOLOGY_YEAR_IMPACT' : 1.5,
	'TECH_ESPIONAGE_BASE_CHANCE' : 2,
	'TECH_ESPIONAGE_TECH_PICK_FACTOR' : 0.3,
	'SPY_MILITARY_INTEL_TIME_BASE' : 32,
	'MILITARY_ESPIONAGE_DETECTION_CHANCE' : 0.6,
	'MILITARY_ESPIONAGE_AT_BASE_CHANCE' : 2,
	'SPY_MILITARY_INTEL_LOCAL_TIME_BASE' : 15
}

economy = {
	'MAX_PROVINCE_SELL_PRICE' 	: 2000,
	'LEADERSHIP_TO_DIPLOMACY' 	: 0.5,	 # Leadership to Diplomatic Influence factor.
	'IC_TO_MONEY' 			: 0.05,
	'IC_TO_CONSUMER_GOODS' 		: 5.0,
	'IC_TO_SUPPLIES' 			: 7.0,
	'CONVOY_BUILD_COST' 		: 2,
	'CONVOY_BUILD_TIME' 		: 100,
	'ESCORT_BUILD_COST' 		: 4,
	'ESCORT_BUILD_TIME' 		: 240,
	'LEADERSHIP_TO_SPIES' 		: 0.15,
	'BUILDING_REPAIR_SPEED' 		: 0.1,
	'LEADERSHIP_TO_OFFICERS' 		: 4,
	'THREAT_FROM_CONVOYS_MODIFIER'	: 0.005,
	'CONVOY_CONSTRUCTION_SIZE' 	: 10,
	'MAX_DAILY_TRADE'			: 100,
	'CONVOY_PATH_LENGTH_MULT'		: 0.33,  # Convoy path length effect on needed transports
	'CONVOY_TRADE_WEIGHT_MULT'	: 0.2,	 #- Trade route convoy effect on needed transports
	'RESOURCE_TO_IC_COST'			: 0.25,	 # For calc IC from damage done to resources (for strat warfare overview)
	'CARGO_TONS_SUNK_SCALE'		: 1000.0,
	'LL_CONVOY_EFF_IMPACT'		: 0.4,  # How much decreases efficiency when convoy is sunk. (f.ex. if impact is 25% and sunk only a half of total transports, then efficiency reduces by 12.5%)
	'LL_CONVOY_EFF_REGAIN'		: 0.01 	 # How much efficiency % regenerates every day.
}

military = {
	'MAX_MANPOWER' : 10,
	'HISTORICAL_MODEL_MAX' : 10, 	# historical models max
	'BASE_CHANCE_TO_AVOID_HIT' : 70.0,	# Base chance to avoid hit if defences left.
	'CHANCE_TO_AVOID_HIT_AT_NO_DEF' : 48.0,	# chance to avoid hit if no defences left.
	'RIVER_CROSSING_PENALTY' : -0.6,	# base river crossing penalty.
	'AMPHIBIOUS_LANDING_PENALTY' : -0.7,	# amphibious landing penalty
	'PARADROP_PENALTY' : -0.5,	# paradrop penalty
	'MULTIPLE_COMBATS_PENALTY' : 0.5,	#- multiple combat modifier
	'ENVELOPMENT_PENALTY' : 0.1,	# envelopment penalty, for each direction.
	'ENCIRCLED_PENALTY' : 0.1,	# for completely encircled units.
	'DIG_IN_FACTOR' : 0.02,	# dig-in factor.
	'COMBAT_SUPPLY_LACK_IMPACT' : 0.5,	#-combat penalty factor on lack of supplies.
	'COMBAT_DISSENT_IMPACT' : 0.02,	#- dissent modifier in combat.
	'PRIDE_SUNK_DISSENT_IMPACT' : 2,	#- Pride of the Fleet Sunk impact
	'SUPPLY_TAX' : 0.1,	#- supplies cost for moving through a province.
	'PARTISAN_EFFECT_ON_SUPPLY_TAX' : 0.1,
	'SUPPLYPOOL_DAYS' : 15,	#- days of supply.
	'SUPPLY_SAME_AREA_DIST_CUTOFF' : 10, # No convoys to within this many provinces from the main supplier province
	'PARASUPPLYPOOL_DAYS' : 7, # when paradropped
	'RADAR_RANGE' : 0.18,
	'BASE_RADIO_STRENGTH' : 10,
	'EXP_GAIN_LAND' : 0.4,
	'EXP_GAIN_NAVAL' : 5.0,
	'EXP_GAIN_AIR' : 0.25,
	'EXP_GAIN_DIV' : 1.8,
	'EXP_GAIN_LEADER' : 240.0,
	'EXP_GAIN_CONVOY_MODIFIER' : 0.25, #- general modifier for gained exp by convoy raiding.
	'PRIDE_BONUS_EXP' : 10,
	'PRIDE_BASE_REDESIGNATION_DISSENT' : 1,
	'LAND_SPEED_MODIFIER' : 0.05,
	'NAVAL_SPEED_MODIFIER' : 0.5,
	'AIR_SPEED_MODIFIER' : 0.3,	
	'MINIMUM_STRENGTH' : 50,		#-minimum strength of a land division at production.  50 : 5000 men.
	'BRIGADES_IN_DIVISION' : 4,	#-number of allowed brigades in a division.
	'COMBAT_LEADER_IMPACT' : 0.05,	
	'COMBAT_MOVEMENT_SPEED' : 0.33,	
	'COMBAT_PUSHBACK_DAMAGE' : 1.0,	
	'COMBAT_PUSHBACK_CHANCE_FOR_DAMAGE' : 50,	#-- 10 : 10%
	'UNIT_ATTACK_DELAY' : 168,
	'UNIT_ATTACK_DELAY_PERIOD' : 12, #  if combat time below this attack delay scales downwards
	'UNIT_ATTACK_DELAY_MODIFY' : 10,
	'PARATROOP_MISSION_DELAY' : 168, 	# Paratroops cannot make another attack drop before this many hours
	'PARATROOP_DROP_ORG_MULT' : 0.66, #- Paratroop org is multiplied with this after they have landed (and combat is over)
	'PARATROOP_DROP_ORG_MULT_PLANE' : 0.01, #- transport planes get their org multiplied with this after performing a mission
	'PARATROOP_MISSION_ORG_REQ' : 0.95, # Paratroop brigades need at least this percent of max org to perform drops
	'UNIT_DIGIN_CAP' : 10,
	'BOMB_STRATEGIC_RESOURCES_INFRA_FRACTION' : 0.075    , # As Less infra as more damage causes bombing to supplies (this factor makes the difference more or less noticable)
	'BOMB_STRATEGIC_RESOURCES' : 0.9,
	'BOMB_STRATEGIC_PRODUCTION' : 0.05,
	'BOMB_STRATEGIC_BUILDINGS' : 0.05,
	'BOMB_REGIMENT_DAMAGE_MODIFIER' : 1.85, #-- bombing ground units (damage modifier > 1 makes the bomb attack stronger)
	'BOMB_SHIP_DAMAGE_MODIFIER' : 1.05, # bombing ships
	'BOMB_WING_DAMAGE_MODIFIER' : 1.05, #- bombing airplanes (on ground)
	'OFFICER_COMBAT_LOSS' : 0.007,
	'AGGRESSIVE_ORGANISATION_LIMIT' : 0.20,
	'DEFENSIVE_ORGANISATION_LIMIT' : 0.30,
	'PASSIVE_ORGANISATION_LIMIT' : 0.50,
	'AGGRESSIVE_STRENGTH_LIMIT' : 0.25,
	'DEFENSIVE_STRENGTH_LIMIT' : 0.50,	
	'PASSIVE_STRENGTH_LIMIT' : 0.75,
	'CAG_DUTY_MINIMUM_ORG_LIMIT' : 0.20,
	'LAND_COMBAT_THREAT_IMPACT' : 0.01,
	'NAVAL_COMBAT_THREAT_IMPACT' : 0.01,
	'AIR_COMBAT_THREAT_IMPACT' : 0.01,
	'BOMBING_THREAT_IMPACT' : 0.01,
	'LAND_DOCTRINE_INCREASE' : 0.001,
	'NAVAL_DOCTRINE_INCREASE' : 0.01,
	'AIR_DOCTRINE_INCREASE' : 0.01,
	'BOMBING_DOCTRINE_INCREASE' : 0.01,
	'COMBAT_DIFFICULTY_IMPACT' : 0.2,
	'BASE_COMBAT_WIDTH' : 10,
	'ADDITIONAL_COMBAT_WIDTH' : 5,
	'RETREAT_PROGRESS' : 0.9,
	'BASE_NIGHT_PENALTY' : -0.5,
	'BASE_FORT_PENALTY' : -0.09,
	'BASE_STACKING_PENALTY' : -0.1,
	'RADAR_COMBAT_IMPACT' : 0.05,
	'COMBAT_EVENT_DURATION' : 8,
	'STRAT_RAIDER_FOUGHT_IMPACT' : 0.015,
	'STRAT_BOMBER_FOUGHT_IMPACT' : 0.1,
	'STRAT_BOMBER_NONCRIT_IMPACT_FRACTION' : 0.25, #-non cores affect unity less
	'STRAT_AIR_RAIDER_FOUGHT_IMPACT' : 0.02,
	'STRAT_NO_ALLIES_HELP_PENALTY' : -0.001,
	'STRAT_ALLIES_HELP' : 0.1,
	'STRAT_BOMBING_PENALTY' : -0.05,
	'STRAT_CONVOY_DAMAGE' : -0.5,
	'LAND_COMBAT_ORG_DICE_SIZE' : 4,
	'LAND_COMBAT_STR_DICE_SIZE' : 2,
	'LAND_COMBAT_STR_ARMOR_ON_SOFT_DICE_SIZE' : 2,
	'LAND_COMBAT_ORG_ARMOR_ON_SOFT_DICE_SIZE' : 5,
	'LAND_COMBAT_STR_DAMAGE_MODIFIER' : 0.6,
	'LAND_COMBAT_ORG_DAMAGE_MODIFIER' : 7.5,
	'AIR_COMBAT_ORG_DICE_SIZE' : 9,
	'AIR_COMBAT_STR_DICE_SIZE' : 4,
	'AIR_COMBAT_CRITICAL_HIT_DAMAGE_MUL' : 10,
	'AIR_COMBAT_CRITICAL_HIT_DAMAGE_CHANCE' : 10,
	'AIR_COMBAT_ORG_DAMAGE_MODIFIER' : 12.5,
	'AIR_COMBAT_STR_DAMAGE_MODIFIER' : 10.0,
	'AIR_COMBAT_NAV_SURPRISE_CHANCE' : 25, #- Chance of attacking naval bombers getting a surprise bonus (%)
	'AIR_COMBAT_NAV_SURPRISE_BONUS' : 0.75, #- Naval bomber surprise attack bonus
	'AIR_COMBAT_NAV_SURPRISE_ROUNDS' : 3, #- Combat rounds that the naval bomber surprise chance lasts
	'AIR_COMBAT_CAG_ORG_DAMAGE_MOD' : 0.75, #- ORG damage to CAGs is multiplied by this
	'AIR_COMBAT_CAG_STR_DAMAGE_MOD' : 0.75, #-- STR damage to CAGs is multiplied by this
	'NAVAL_COMBAT_ORG_DICE_SIZE' : 1,
	'NAVAL_COMBAT_STR_DICE_SIZE' : 10,
	'NAVAL_COMBAT_CRITICAL_HIT_DAMAGE_MUL' : 10,
	'NAVAL_COMBAT_CRITICAL_HIT_DAMAGE_CHANCE' : 10,
	'NAVAL_COMBAT_ORG_DAMAGE_MODIFIER' : 1.0, #- Average damage is the same, but it will be more consistent with less randomness.
	'NAVAL_COMBAT_STR_DAMAGE_MODIFIER' : 0.12, #- Average Strength damage is a bit higher and there will be much more randomness in it!
	'NAVAL_COMBAT_SUB_SURPRISE_CHANCE' : 30, # Chance of attacking subs getting a surprise bonus (%)
	'NAVAL_COMBAT_SUB_SURPRISE_BONUS' : 5.0, # Sub surprise attack bonus
	'NAVAL_COMBAT_SUB_SURPRISE_ROUNDS' : 3, # Combat rounds that the sub surprise chance lasts
	'AIR_COMBAT_ON_BOMBING' : -0.1,
	'BASE_PROXIMITY_BONUS' : 0.1,
	'INTERCEPT_ATTACK_BONUS' : 0.2,
	'AIR_SUP_DEFEND_BONUS' : 0.2,
	'SHORE_BOMBARDMENT_MOD' : -0.01,
	'SHORE_BOMBARDMENT_CAP' : -0.25,
	'AIR_STACKING_PENALTY' : -0.10,
	'NAVAL_STACKING_PENALTY' : -0.02,			#- OBSOLETE
	'NAVAL_STACK_POS_HULL_LIMIT' : 16,		#- Total Hull size up to this is fine before taking stacking penalty
	'NAVAL_STACK_POS_PENALTY_HULL_MULT' : 0.04, 	#- Every hull point above the limit gives this positioning penalty
	'NAVAL_STACK_POS_PENALTY_MAX' : 0.8, #- maximum possible penalty due to size/positioning
	'AIR_STACKING_PENALTY_MAX' : -0.8,
	'STRAT_REDEP_BASE_SPEED' : 20,
	'STRAT_REDEP_SUPPLY_MOD' : 2.0,
	'STRAT_REDEP_ORG_LOSS' : 0.02,
	'AIR_RANK_1' : 4,
	'AIR_RANK_2' : 8,
	'AIR_RANK_3' : 12,
	'AIR_RANK_4' : 16,
	'NAVAL_RANK_1' : 6,
	'NAVAL_RANK_2' : 12,
	'NAVAL_RANK_3' : 18,
	'NAVAL_RANK_4' : 30,
	'RADIO_CORPS_LEADER_DISTANCE' : 100,
	'RADIO_ARMY_LEADER_DISTANCE' : 200,
	'RADIO_ARMYGROUP_LEADER_DISTANCE' : 300,
	'RADIO_THEATHRE_LEADER_DISTANCE' : 1000,
	'OWNED_AND_CONTROLLED_THROUGHPUT_CAP_BONUS' : 2,
	'INFRA_THROUGHPUT_IMPACT' : 4,
	'SURPRISE_BONUS' : 0.33,
	'STATIC_AA_SCALE' : 5.0,
	'AIR_SUPPLY_FACTOR' : 0.5,
	'UNIT_UPGRADE_COST' : 0.4, #- was 0.1 and before was 0.5
	'UNIT_UPGRADE_TIME' : 0.7,
	'UNIT_UPGRADE_PRACTICAL_MOD' : 0.9, #- smaller value gives less practical on upgrade
	'MAX_OFFICERS' : 1.4, # officer ratio max for bonus
	'LOW_ORG_REGAIN_BONUS' : 0.3,
	'SPAWN_PARTISAN_LIMIT' : 0.5,
	'SPAWN_UNDERGROUND_LIMIT' : 1,
	'MAX_UNDERGROUND_DISTANCE' : 1000,
	'UNDERGOUND_INITIAL_STRENGTH' : 0.4,
	'UNDERGROUND_STRENGTH_GAIN' : 0.01,
	'UNDERGROUND_DETECT_CHANCE' : 2,
	'UNDERGROUND_PARTISAN_STRENGTH' : 1,
	'UNDERGROUND_SPAWN_STRENGTH' : 0.2,
	'PARADROP_MIN_TIME' : 24,
	'NAVAL_INTERCEPTION_AFTER_ATTACK_FACTOR' : 1.3,
	'NAVAL_BASE_EFFICIENCY' : 6, # base throughput cap on ports
	'AMPHIBIOUS_INVADE_SPEED_BASE' : 0.5, #- every hour movement progress on amphibious invasion
	'AMPHIBIOUS_INVADE_MOVEMENT_COST' : 24.0, #- total progress cost of movement while amphibious invading
	'AMPHIBIOUS_INVADE_ATTACK_LOW' : 0.2, #- low and high cap of attack modifier scale. Scale interpolated by invasion progress.
	'AMPHIBIOUS_INVADE_ATTACK_HIGH' : 1.0,
	'AMPHIBIOUS_INVADE_DEFEND_LOW' : 1.5, # low and high cap of defend modifier scale. Scale interpolated by invasion progress.
	'AMPHIBIOUS_INVADE_DEFEND_HIGH' : 1.0,
	'AMPHIBIOUS_INVADE_LANDING_PENALTY_DECREASE' : 3.5, #- scale of bonus that decreases "amphibious penalty" during combat, relative to invading transporter tech.
	'LAND_COMBAT_STR_ARMOR_DEFLECTION_FACTOR' : 0.5, #- damage reduction if armor outclassing enemy
	'LAND_COMBAT_ORG_ARMOR_DEFLECTION_FACTOR' : 0.5, #- damage reduction if armor outclassing enemy
	'TACTIC_SWAP_FREQUENCEY' : 24, #- hours of combat before tactis are automatically changed
	'INITIATIVE_PICK_COUNTER_ADVANTAGE_FACTOR' : 0.35, #- higher chance factor of picking a counter tactic if has initiative
	'AGGRESSIVNESS_SELECTION_IMPACT' : 0.5, #- effect of aggressivness slider on tactic selection
	'SHIP_UPGRADE_SPEED_MOD' : 4.0,
	'AIR_UPGRADE_SPEED_MOD' : 1.0, 
	'LAND_UPGRADE_SPEED_MOD' : 1.0,
	'CAG_STACKING_PENALTY' : 0.5,
	'CAG_SHIP_ATTACK_STR_BONUS' : 2.0, #- attack bonus if CAG attacks ships who are busy in combat
	'CAG_SHIP_ATTACK_ORG_BONUS' : 2.0,
	'NEW_LEADER_ORG_HIT' : 0.5
}

diplomacy = {
	'WARDEC_BELIGERENCY' : 33.0, 
	'WARDEC_WAR_DIPLOMACY_HIT' : 100.0,
	'WARDEC_INFLUENCE_COST' : 0.0,
	'JOIN_ALLIANCE_INFLUENCE_COST' : 5.0,
	'LEAVE_ALLIANCE_INFLUENCE_COST' : 1.0,
	'GUARANTEE_INFLUENCE_COST' : 5.0,
	'REVOKE_GUARANTEE_INFLUENCE_COST' : 1.0,
	'CALLALLY_INFLUENCE_COST' : 1.0,
	'NAP_INFLUENCE_COST' : 5.0,
	'PURCHASE_INFLUENCE_COST' : 0.0,
	'EMBARGO_INFLUENCE_COST' : 1.0,
	'MILACCESS_INFLUENCE_COST' : 1.0,
	'INFLUENCE_INFLUENCE_COST' : 3.0,
	'RELATION_INFLUENCE_COST' : 1.0,
	'JOINBLOCK_INFLUENCE_COST' : 5.0,	
	'ALLIANCE_NEUTRALITY_LIMIT' : 25.0,
	'ALLIANCE_RELATION_CHANGE' : 15.0,
	'ALLIANCE_REJECT_RELATION_CHANGE' : -50.0,
	'GUARANTEE_NEUTRALITY_LIMIT' : 60.0,
	'NAP_RELATION_CHANGE' : 50.0,
	'LEAVE_NAP_RELATION_CHANGE' : -30.0,
	'NAP_JOIN_INFLUENCE_COST' : 5.0,
	'NAP_REJECT_RELATION_CHANGE' : -20.0,
	'LEAVE_NAP_INFLUENCE_COST' : 1.0,
	'LEAVE_NAP_THREAT_COST' : 5.0,
	'MILACC_ACCEPT_RELATION_CHANGE' : 10,
	'MILACC_DECLINE_RELATION_CHANGE' : -10,
	'MILACC_CANCEL_RELATION_CHANGE' : -20,
	'REVOKE_GUARANTEE_RELATION_CHANGE' : -20,
	'DAYS_OF_INFLUENCE_RELATION_CHANGE' : -1,
	'DAYS_OF_ALIGN_RELATION_CHANGE' : -1,
	'ALIGN_INFLUENCE_COST' : 1.0,
	'EMBARGO_RELATION_CHANGE' : -30.0,
	'EMBARGO_NETRALITY' : 65.0,
	'EMBARGO_THREAT_COST' : 1.0,
	'JOIN_FACTION_INFLUENCE_COST' : 0,
	'INVITE_FACTION_INFLUENCE_COST' : 0,
	'JOIN_FACTION_NEUTRALITY' : 25.0,
	'INVITE_FACTION_NEUTRALITY' : 25.0,
	'TRADE_RELATION_CHANGES' : 15.0,
	'TRADE_INFLUENCE_COST' : 3.0,
	'TRADE_CANCEL_INFLUENCE_COST' : 1.0,
	'TRADE_CANCEL_RELATION_COST' : 15.0,
	'EXPEDITION_INFLUENCE_COST' : 1.0,
	'EXPEDITION_RETURN_TIME' : 30.0,
	'EXPEDITION_RECLAIM_TIME' : 30.0,
	'LICENCE_INFLUENCE_COST' : 1.0,
	'ALLOW_DEBT_INFLUENCE_COST' : 1.0, 
	'REVOKE_DEBT_INFLUENCE_COST' : 1.0,
	'YEARS_TO_REPAY_DEBT' : 50.0,
	'INFLUENCE_UPKEEP' : 1.0,
	'REGULAR_CONSTRUCTION_THREAT_IMPACT' : 0.1,
	'NEUT_INCREASE_DIFF_CONTINENT' : 10,
	'NEUT_REDUCTION_AT_CLAIMS' : 20,
	'SHARE_TECH_INFLUENCE_COST' : 1.0,
	'SHARE_TECH_LEADERSHIP_COST' : 1.0,
	'NAP_EXPIRY_MONTHS' : 45,				#-- NAPs expire after this many months
	'NAP_UNBREAKABLE_MONTHS' : 6,			#-- NAPS cannot be broken for this many months
	'NAP_FORCE_BALANCE_RULE_MONTHS' : 6,		#-- The NAP border force balance rule changes with this interval
	'NAP_BREAK_FORCE_BALANCE_1' : 2.0,		#-- 2-1 brigades along the border required to break NAP
	'NAP_BREAK_FORCE_BALANCE_2' : 1.0,		#-- 1-1 brigades along the border required to break NAP
	'NAP_BREAK_FORCE_BALANCE_3' : 0.5,		#-- 1-2 brigades along the border required to break NAP
	'NAP_EXPIRY_ALERT_MONTHS_AHEAD' : 3, # -- That many months ahead the alert will appear.
	'WARGOAL_ADD_COOLDOWN' : 1, #-- 1  month
	'LEND_LEASE_NEUTRALITY_LIMIT' : 60.0,
	'LEND_LEASE_MAX_IC_LOW' : 0.05,	#-- bounds of % of our IC that we can share with LL. Interpolated by current neutrality vs LEND_LEASE_NEUTRALITY_LIMIT aspect.
	'LEND_LEASE_MAX_IC_HIGH' : 0.90
}

alignment = {
	'ALIGNMENT_INTERVAL' : 24,
	'RELATION_WEIGHT' : 0.02,
	'IDEOLOGY_WEIGHT' : 2.00,
	'PROXIMITY_WEIGHT' : 0.03,
	'REVANCHISM_WEIGHT' : 0.1,
	'THREAT_WEIGHT' : 0.15,
	'REPULSION_WEIGHT' : 0.01,
	'FACTION_JOIN_DIST' : 50.0,
	'FACTION_STRAT_BONUS_DIST' : 100.0,
	'FACTION_THREAT_DIST' : 400.0,
	'WAR_THREAT' : 35.0,
	'LARGE_COUNTRY_IC' : 300.0, #-- used to scale threat impact with country IC
	'REPULSION_IC_FACTOR' : 8	#-- If a country is of another ideology, the repulsion factor is modified by this value times its max IC, divided by the faction IC
}

maps = {
	'SUEZ' : 11381,
	'SUEZ_BLOCKER' : 5612,
	'PANAMA' : 11383,
	'PANAMA_BLOCKER' : 7717,
	'BALTIC' : 10517,
	'BALTIC_BLOCKER' : 1482,
	'BLACKSEA' : 11382,
	'BLACKSEA_BLOCKER' : 4503,
	'GIBRALTAR' : 10559,
	'GIBRALTAR_BLOCKER' : 5191
}

weather = {
	'PRESSUREMIN' 			: 870,	                           
	'PRESSUREMAX' 			: 1090,                                   
	'PRESSUREDEFAULT' 		: 1013,                                   
	'PRESSURESTEP'			: 5,                                      
	'PRESSUREREDUCTION'		: 2,                                      
	'PRESSUREPROPAGATION' 	: 10,                                     
	'PRESSURETHRESHOLD' 		: 300,                                    
	'MAXHUMIDITY' 			: 100,                                    
	'MINHUMIDITY' 			: 0,                                      
	'MAXFROMEACHPRESSURE' 	: 8,                                      
	'HIGHTEMPERATUREATTRITIONTHRESHOLD' 	: 30,     
	'LOWTEMPERATUREATTRITIONTHRESHOLD' 	: -10,    
	'WINDATTRITIONTHRESHOLD' 				: 30,     
	'CLOUDCOVERAGETEMPERATUREDROP' 		: 8,      
	'LANDRAINIMPACTMODIFIER'				: 0.005, 
	'LANDRAINIMPACTCAP' 					: 0.90,   
	'LANDLOWTEMPERATURETHRESHOLD' 		: -1,     
	'LANDLOWTEMPERATUREIMPACT'			: 0.75,    
	'LANDHIGHTEMPERATURETHRESHOLD' 		: 20,     
	'LANDHIGHTEMPERATUREIMPACT' 			: 0.07,    
	'AIRLOWTEMPERATURETHRESHOLD' 			: -5,     
	'AIRLOWTEMPERATUREIMPACT' 			: 0.2,    
	'AIRHIGHTEMPERATURETHRESHOLD' 		: 30,   
	'AIRHIGHTEMPERATUREIMPACT' 			: 0.2,  
	'AIRWINDSPEEDMODIFIER' 				: 0.025,
	'AIRCLOUDMODIFIER' 					: 0.25, 
	'AIRRAINMODIFIER' 					: 0.05, 
	'BOMBLOWTEMPERATURETHRESHOLD' 		: -5,   
	'BOMBLOWTEMPERATUREIMPACT' 			: 0.2,  
	'BOMBHIGHTEMPERATURETHRESHOLD' 		: 30,   
	'BOMBHIGHTEMPERATUREIMPACT' 			: 0.2,  
	'BOMBWINDSPEEDMODIFIER' 				: 0.025,
	'BOMBCLOUDMODIFIER' 					: 0.25,  
	'BOMBRAINMODIFIER' 					: 0.05,  
	'NAVALWINDSPEEDMODIFIER' 				: 0.001,  
	'NAVALRAINMODIFIER' 					: 0.33,  
	'MUDDYNESSMOVEMENTMODIFIER' 			: -0.75, 
	'COLDMOVEMENTMODIFIER'				: -0.75,
	'HOURLYFROZENINCREASE' 				: 0.01,  
	'HOURLYTHAW' 							: 0.01,  
	'TEMPERATURECHANGESPEED' 				: 0.1,   
	'WEATHERMOVEMENTDELAY' 				: 2,     
	'LOWPRESSUREBASE' 					: 100,   
	'LOWPRESSUREOFFSET' 					: 100,   
	'ALLOWEDTOFLYTHRESHOLD' 				: 0.8,   
	'SPOTTINGCLOUDMODIFIER' 				: 0.2,   
	'SPOTTINGRAINMODIFIER' 				: 0.2,   
	'MUDDYNESSSUPPLYTAXMODIFIER'			: 0.25,  
	'FIRINGRANGEMODIFIER'				: -0.5,
	'GFX_RAINIMPACT_LIMIT'				: 0.1,
	'GFX_RAIN_LIMIT'						: 0.1,
	'GFX_SNOW_LIMIT'						: 0.1,
	'GFX_STORM_LIMIT'						: 15.0,
	'GFX_SNOW_STORM_LIMIT'				: 15.0,
	'GFX_PARTIAL_CLOUD_LIMIT'				: 0.3,
	'GFX_CLOUD_LIMIT'						: 0.8,
	'INITIAL_SIMULATION_HOURS_AHEAD'		: 96  #-- (24*4) 4 days ahead - reducing it may improve loading time a bit, but worse weather precision at startup
}

goods_cost = {
	'SUPPLIES' 		: 0.25,
	'FUEL' 			: 0.75,
	'MONEY' 			: 0.0,
	'CRUDE_OIL' 		: 0.75,
	'METAL' 			: 0.1,
	'ENERGY' 			: 0.05,
	'RARE_MATERIALS' 		: 0.2
}
industry_tech =  {
'combat_medicine' : {
	'casualty_trickleback' : 0.01,
	'research_bonus_from' : {
		'land_doctrine_practical' : 1.0
	},
	'on_completion' : 'land_doctrine_practical',
	'difficulty' : 2,
	'start_year' : 1938,
	'first_offset' : 1939,	#2nd model is from 1936,
	'additional_offset' : 3,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'industry_folder'
}	,

'first_aid' : {
	'maximum_attrition' : -0.05,
	'research_bonus_from' : {
		'land_doctrine_practical' : 1.0
	},
	'on_completion' : 'land_doctrine_practical',
	'difficulty' : 2,
	'start_year' : 1938,
	'first_offset' : 1939,	#2nd model is from 1936,
	'additional_offset' : 3,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'industry_folder'
}	,

'agriculture' : {
	'manpower_gain' : 0.10,
	'research_bonus_from' : {
		'mechanicalengineering_theory' : 1.0
	},
	'on_completion' : 'mechanicalengineering_theory',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1932,	#2nd model is from 1936,
	'additional_offset' : 2,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'industry_folder'
}	,

'industral_production' : {
	'ic_modifier' : 0.025,
	'research_bonus_from' : {
		'mechanicalengineering_theory' : 0.5,
		'construction_practical'  : 0.5
	},
	'on_completion' : 'mechanicalengineering_theory',
	'difficulty' : 2,
	'start_year' : 1918,
	'first_offset' : 1932,	#2nd model is from 1936,
	'additional_offset' : 2,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'industry_folder'
}	,

'industral_efficiency' : {
	'ic_efficiency' : 0.025,
	'research_bonus_from' : {
		'mechanicalengineering_theory' : 0.5,
		'construction_practical' : 0.5
	},
	'on_completion' : 'mechanicalengineering_theory',
	'difficulty' : 2,
	'start_year' : 1918,
	'first_offset' : 1932,	#2nd model is from 1936,
	'additional_offset' : 2,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'industry_folder'
}	,

'oil_to_coal_conversion' : {
	'energy_to_oil_conversion' : 0.1,
	'research_bonus_from' : {
		'chemical_engineering' : 1.0,
	},
	'on_completion' : 'chemical_engineering',
	'difficulty' : 5,
	'start_year' : 1936,
	'first_offset' : 1938,	#2nd model is from 1936,
	'additional_offset' : 3,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'industry_folder'
}	,

'supply_production' : {
	'ic_to_supplies' : 0.05,
	'research_bonus_from' : {
		'mechanicalengineering_theory' : 1.0
	},
	'on_completion' : 'mechanicalengineering_theory',
	'difficulty' : 2,
	'start_year' : 1918,
	'first_offset' : 1932,	#2nd model is from 1936,
	'additional_offset' : 2,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'industry_folder'
}	,

'heavy_aa_guns' : {
	'activate_building' : 'anti_air' ,
	'provincial_aa_efficiency' : 0.2,
	'research_bonus_from' : {
		'artillery_theory' : 0.3,
		'artillery_practical' : 0.7
	},
	'on_completion' : 'artillery_theory' ,
	'difficulty' : 2,
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936,
	'additional_offset' : 3,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'industry_folder'
},
	
'electronic_mechanical_egineering' : {
	'research_bonus_from' : {
		'electornicegineering_theory'  : 0.5,
		'mechanicalengineering_theory' : 0.5
	},
	'on_completion' : 'electornicegineering_theory',
	'difficulty' : 2,
	'start_year' : 1926,
	'folder' : 'industry_folder'
},

'radio_technology' : {
	'allow' : {
		'electronic_mechanical_egineering' : 1
	},
	'research_bonus_from' : {
		'electornicegineering_theory'  : 0.5,
		'mechanicalengineering_theory' : 0.5
	},
	'on_completion' : 'electornicegineering_theory',
	'difficulty' : 2,
	'start_year' : 1930,
	'folder' : 'industry_folder',
},

'radio_detection_equipment' : {
	'listening_station' : 'yes',
	'allow' : {
		'radio_technology' : 1
			},
	'research_bonus_from' : {
		'electornicegineering_theory'  : 0.5,
		'mechanicalengineering_theory' : 0.5
	},
	'on_completion' : 'electornicegineering_theory',
	'difficulty' : 2,
	'start_year' : 1936,
	'folder' : 'industry_folder'
},

'radio' : {
	'combat_efficiency' : 0.1,
	'encryption' : -0.2,
	'allow' : {
		'radio_technology' : 1
	},
	'research_bonus_from' : {
		'electornicegineering_theory'  : 0.5,
		'mechanicalengineering_theory' : 0.5
	},
	'on_completion' : 'electornicegineering_theory',
	'difficulty' : 2,
	'start_year' : 1936,
	'folder' : 'industry_folder'
},

'radar' : {
	'activate_building' : 'radar_station' ,
	'radar_efficiency' : 0.05 ,
	'allow' : {
		'radio_technology' : 1
	},
	'research_bonus_from' : {
		'electornicegineering_theory'  : 0.5,
		'electornicegineering_practical' : 0.5
	},
	'on_completion' : 'electornicegineering_theory',
	'difficulty' : 3,
	'start_year' : 1939,
	'first_offset' : 1940,	#2nd model is from 1940,
	'additional_offset' : 2,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'industry_folder'
},

'census_tabulation_machine' : {
	'research_efficiency' : 0.02,
	'allow' : {
		'electronic_mechanical_egineering' : 1
	},
	'research_bonus_from' : {
		'mechanicalengineering_theory' : 1.0
	},
	'on_completion' : 'mechanicalengineering_theory',
	'difficulty' : 2,
	'start_year' : 1936,
	'folder' : 'industry_folder'
},

'mechnical_computing_machine' : {
	'research_efficiency' : 0.02,
	'allow' : {
		'census_tabulation_machine' : 1
	},
	'research_bonus_from' : {
		'mechanicalengineering_theory' : 1.0
	},
	'on_completion' : 'mechanicalengineering_theory',
	'difficulty' : 3,
	'start_year' : 1938,
	'first_offset' : 1940 ,
	'additional_offset' : 3,
	'max_level' : 12,
	'folder' : 'industry_folder'
},

'electronic_computing_machine' : {
	'research_efficiency' : 0.05,
	'allow' : {
		'mechnical_computing_machine' : 2
	},
	'research_bonus_from' : {
		'electornicegineering_theory' : 1.0
	},
	'on_completion' : 'electornicegineering_theory',
	'difficulty' : 4,
	'start_year' : 1941,
	'first_offset' : 1943,
	'additional_offset' : 3,
	'max_level' : 12,
	'folder' : 'industry_folder'
},

'decryption_machine' : {
	'decryption' : 0.1,
	'allow' : {
		'OR' : {
			'mechnical_computing_machine' : 1,
			'AND' : {
				'NOT' : { 'decryption_machine' : 3 },
				'electronic_computing_machine' : 1
			}
		}
	},
	'research_bonus_from' : {
		'electornicegineering_theory'  : 0.5,
		'mechanicalengineering_theory' : 0.5
	},
	'on_completion' : 'electornicegineering_theory',
	'difficulty' : 2,
	'start_year' : 1938,
	'first_offset' : 1940,	#2nd model is from 1936,
	'additional_offset' : 3,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'industry_folder'
},

'encryption_machine' : {
	'encryption' : 0.1,
	'allow' : {
		'OR' : {
			'mechnical_computing_machine' : 1,
			'AND' : {
				'NOT' : { 'encryption_machine' : 3 },
				'electronic_computing_machine' : 1
			}
		}
	},
	'research_bonus_from' : {
		'electornicegineering_theory'  : 0.5,
		'mechanicalengineering_theory' : 0.5
	},
	'on_completion' : 'electornicegineering_theory',
	'difficulty' : 2,
	'start_year' : 1938,
	'first_offset' : 1940,	#2nd model is from 1936,
	'additional_offset' : 3,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'industry_folder'
},

'construction_engineering' : {
	'activate_building' : 'industry',
	'research_bonus_from' : {
		'mechanicalengineering_theory' : 0.5,
		'construction_practical' : 0.5
	},
	'on_completion' : 'mechanicalengineering_theory',
	'difficulty' : 2,
	'start_year' : 1918,
	'folder' : 'industry_folder'
}	,

'advanced_construction_engineering' : {
	'activate_building' : 'infra',
	'allow' : {
		'industral_production' : 3,
		'industral_efficiency' : 3
	},
	'research_bonus_from' : {
		'mechanicalengineering_theory' : 0.5,
		'construction_practical' : 0.5
	},
	'on_completion' : 'mechanicalengineering_theory',
	'difficulty' : 2,
	'start_year' : 1938,
	'folder' : 'industry_folder'
}	,

'rocket_tests' : {
	'activate_building' : 'rocket_test' ,
	'research_bonus_from' : {
		'rocket_science' : 1.0
	},
	'on_completion' : 'rocket_science',
	'difficulty' : 4,
	'start_year' : 1936,
	'folder' : 'industry_folder'
}	,

'rocket_engine' : {
	'allow' : {
		'rocket_tests' : 1,
		'any_owned_province' : {
			'has_building' : 'rocket_test'
		}
	},
	'research_bonus_from' : {
		'rocket_science' : 1.0
	},
	'on_completion' : 'rocket_science',
	'difficulty' : 4,
	'start_year' : 1939,
	'folder' : 'industry_folder'
}	,

'theorical_jet_engine' : {
	'allow' : {
		'rocket_engine' : 1
	},
	'research_bonus_from' : {
		'jetengine_theory' : 1.0
	},
	'on_completion' : 'jetengine_theory',
	'difficulty' : 4,
	'start_year' : 1940,
	'folder' : 'industry_folder'
}	,

'atomic_research' : {
	'research_bonus_from' : {
		'nuclear_physics' : 1.0
	},
	'on_completion' : 'nuclear_physics',
	'difficulty' : 10,
	'is_nuclear' : 'yes',
	'start_year' : 1936,
	'folder' : 'industry_folder'
},

'nuclear_research' : {
	'allow' : {
		'atomic_research' : 1
	},
	'research_bonus_from' : {
		'nuclear_physics' : 1.0
	},
	'on_completion' : 'nuclear_physics',
	'difficulty' : 10,
	'is_nuclear' : 'yes',
	'start_year' : 1940,
	'folder' : 'industry_folder'
},

'isotope_seperation' : {
	'allow' : {
		'nuclear_research' : 1
	},
	'research_bonus_from' : {
		'nuclear_physics' : 1.0
	},
	'on_completion' : 'nuclear_physics',
	'difficulty' : 10,
	'is_nuclear' : 'yes',
	'start_year' : 1942,
	'folder' : 'industry_folder'
},
 
'civil_nuclear_research' : {
	'activate_building' : 'nuclear_reactor',
	'allow' : {
		'isotope_seperation' : 1
	},
	'research_bonus_from' : {
		'nuclear_physics' : 1.0
	},
	'on_completion' : 'nuclear_physics',
	'difficulty' : 10,
	'is_nuclear' : 'yes',
	'start_year' : 1943,
	'first_offset' : 1944,
	'additional_offset' : 1,
	'max_level' : 12,
	'folder' : 'industry_folder'
},

'oil_refinning' : {
	'refinery_efficiency' : 0.1,
	'research_bonus_from' : {
		'chemical_engineering' : 1.0
	},
	'on_completion' : 'chemical_engineering',
	'difficulty' : 5,
	'start_year' : 1936,
	'first_offset' : 1938,	#2nd model is from 1936,
	'additional_offset' : 3,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'industry_folder'
},

'steel_production' : {
	'metal_production' : 0.05,
	'research_bonus_from' : {
		'chemical_engineering' : 1.0,
	},
	'on_completion' : 'chemical_engineering',
	'difficulty' : 5,
	'start_year' : 1936,
	'first_offset' : 1938,	#2nd model is from 1936,
	'additional_offset' : 3,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'industry_folder'
},

'raremetal_refinning_techniques' : {
	'rares_production' : 0.05,
	'research_bonus_from' : {
		'chemical_engineering' : 1.0,
	},
	'on_completion' : 'chemical_engineering',
	'difficulty' : 5,
	'start_year' : 1936,
	'first_offset' : 1938,	#2nd model is from 1936,
	'additional_offset' : 3,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'industry_folder'
},

'coal_processing_technologies' : {
	'energy_production' : 0.05,
	'research_bonus_from' : {
		'chemical_engineering' : 1.0
	},
	'on_completion' : 'chemical_engineering',
	'difficulty' : 5,
	'start_year' : 1936,
	'first_offset' : 1938,	#2nd model is from 1936,
	'additional_offset' : 3,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'industry_folder'
},

'education' : {
	'leadership_gain' : 0.05,
	'research_bonus_from' : {
		'land_doctrine_practical' : 1.0
	},
	'on_completion' : 'land_doctrine_practical',
	'difficulty' : 5,
	'start_year' : 1936,
	'first_offset' : 1938,	#2nd model is from 1936,
	'additional_offset' : 3,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'industry_folder'
},

'supply_transportation' : {
	'supply_transfer_cost' : -0.01,
	'research_bonus_from' : {
		'land_doctrine_practical' : 1.0
	},
	'on_completion' : 'land_doctrine_practical',
	'difficulty' : 5,
	'start_year' : 1936,
	'first_offset' : 1938,	#2nd model is from 1936,
	'additional_offset' : 2,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'theory_folder'	
},

'supply_organisation' : {
	'supply_throughput' : 0.05,
	'research_bonus_from' : {
		'land_doctrine_practical' : 1.0
	},
	'on_completion' : 'land_doctrine_practical',
	'difficulty' : 5,
	'start_year' : 1936,
	'first_offset' : 1938,	#2nd model is from 1936,
	'additional_offset' : 2,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'theory_folder'	
},

'civil_defence' : {
	'repair_rate' : 0.05, 	#building repair 5% faster,
	'research_bonus_from' : {
		'land_doctrine_practical' : 1.0
	},
	'on_completion' : 'land_doctrine_practical',
	'difficulty' : 5,
	'start_year' : 1936,
	'first_offset' : 1939,	#2nd model is from 1936,
	'additional_offset' : 3,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'theory_folder'	
}
}
infantry_tech =  {
'cavalry_smallarms' : {
	'cavalry_brigade' : {
		'soft_attack' : 0.5
	},
	'research_bonus_from' : {
		'mobile_theory' : 0.3,
		'mobile_practical' : 0.6,
		'human_wave_theory' : 0.1
	},
	'on_completion' : 'mobile_theory',
	'difficulty' : 0,
	#common for all techs.,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1934,
	'additional_offset' : 2,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'infantry_folder'
},

'cavalry_support'  : {
	'cavalry_brigade' : {
		'defensiveness' : 0.5,
		'maximum_speed' : -0.02
	},
	'research_bonus_from' : {
		'mobile_theory' : 0.3,
		'mobile_practical' : 0.6,
		'militia_practical' : 0.1
	},
	'on_completion' : 'mobile_theory',
	'difficulty' : 0,
	#common for all techs.,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936,
	'additional_offset' : 2,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'infantry_folder'
},

'cavalry_guns' : {
	'cavalry_brigade' : {
		'toughness' : 0.4,
		'maximum_speed' : -0.03
	},
	'research_bonus_from' : {
		'mobile_theory' : 0.3,
		'mobile_practical' : 0.5,
		'artillery_practical' : 0.2,
	},
	'on_completion' : 'mobile_theory',
	'difficulty' : 0,
	#common for all techs.,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936,
	'additional_offset' : 2,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'infantry_folder'
},

'cavalry_at' : {
	'cavalry_brigade' : {
		'hard_attack' : 0.1,
		'maximum_speed' : -0.01,
		'ap_attack' : 0.5
	},

	'research_bonus_from' : {
		'mobile_theory' : 0.3,
		'mobile_practical' : 0.6,
		'artillery_practical' : 0.1
	},
	'on_completion' : 'mobile_theory',
	'difficulty' : 0,
	#common for all techs.,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936,
	'additional_offset' : 2,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'infantry_folder'
},

'militia_smallarms' : {
	'activate_building' : 'land_fort' ,
	'militia_brigade' : {
		'soft_attack' : 0.3
	},
	'partisan_brigade' : {
		'soft_attack' : 0.3
	},
	'garrison_brigade' : {
		'soft_attack' : 0.4
	},
	'research_bonus_from' : {
		'militia_theory' : 0.3,
		'militia_practical' : 0.6,
		'human_wave_theory' : 0.1
	},
	'on_completion' : 'militia_theory',
	'difficulty' : 0,
	#common for all techs.,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936,
	'additional_offset' : 2,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'infantry_folder'
},

'militia_support'  : {
	#TODO: move this somwhere it makes sense,
	'activate_building' : 'underground',
	'militia_brigade' : {
		'defensiveness' : 0.5
	},
	'partisan_brigade' : {
		'defensiveness' : 0.5
	},
	'garrison_brigade' : {
		'defensiveness' : 0.5
	},
	'research_bonus_from' : {
		'militia_theory' : 0.3,
		'militia_practical' : 0.6,
		'infantry_practical' : 0.1
	},
	'on_completion' : 'militia_theory',
	'difficulty' : 0,
	#common for all techs.,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936,
	'additional_offset' : 2,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'infantry_folder'
},

'militia_guns' : {
	'militia_brigade' : {
		'toughness' : 0.1
	},
	'partisan_brigade' : {
		'toughness' : 0.1
	},
	'garrison_brigade' : {
		'toughness' : 0.1
	},

	'research_bonus_from' : {
		'militia_theory' : 0.3,
		'militia_practical' : 0.5,
		'artillery_practical' : 0.2
	},
	'on_completion' : 'militia_theory',
	'difficulty' : 0,
	#common for all techs.,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936,
	'additional_offset' : 2,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'infantry_folder'
},

'militia_at' : {
	'militia_brigade' : {
		'hard_attack' : 0.1,
		'ap_attack' : 0.5
	},
	'partisan_brigade' : {
		'hard_attack' : 0.1,
		'ap_attack' : 0.5
	},
	'garrison_brigade' : {
		'hard_attack' : 0.15,
		'ap_attack' : 0.5
	},

	'research_bonus_from' : {
		'militia_theory' : 0.3,
		'militia_practical' : 0.6,
		'artillery_practical' : 0.1
	},
	'on_completion' : 'militia_theory',
	'difficulty' : 0,
	#common for all techs.,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936,
	'additional_offset' : 2,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'infantry_folder'
},

'infantry_activation' : {
	'activate_unit' : 'infantry_brigade' ,
	'activate_unit' : 'Guards_brigade',
	'allow' : {
		'militia_smallarms' : 1,
		'militia_support' : 1,
		'militia_guns' : 1,
		'militia_at' : 1
	},
	'research_bonus_from' : {
		'infantry_theory' : 0.3,
		'infantry_practical' : 0.7
	},
	'on_completion' : 'infantry_theory',
	'difficulty' : 1,
	'start_year' : 1918,
	'folder' : 'infantry_folder'
},

'smallarms_technology' : {
	#each model gets this added to it.,
	'infantry_brigade' : {
		'soft_attack' : 0.6
	},
	'marine_brigade' : {
		'soft_attack' : 0.6
	},
	'paratrooper_brigade' : {
		'soft_attack' : 0.6
	},
	'bergsjaeger_brigade' : {
		'soft_attack' : 0.6
	},
	'motorized_brigade' : {
		'soft_attack' : 0.6
	},
	'mechanized_brigade': {
		'soft_attack' : 0.6
	},
	'alpini_brigade' : {
		'soft_attack' : 0.6
	},
	'alpins_brigade' : {
		'soft_attack' : 0.6
	},
	'gurkha_brigade' : {
		'soft_attack' : 0.6
	},
	'waffen_brigade' : {
		'soft_attack' : 0.6
	},
	'Guards_brigade' : {
		'soft_attack' : 0.6
	},
	'imperial_brigade' : {
		'soft_attack' : 0.6
	},
	'ranger_brigade' : {
		'soft_attack' : 0.6
	},
	'allow' : {
		'infantry_activation' : 1
	},
	'research_bonus_from' : {
		'infantry_theory' : 0.3,
		'infantry_practical' : 0.6,
		'grand_battleplan_theory' : 0.1
	},
	'on_completion' : 'infantry_theory',
	'difficulty' : 0,
	#common for all techs.,
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936,
	'additional_offset' : 2,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'infantry_folder'
},

'infantry_support' : {
	#each model gets this added to it.,
	'infantry_brigade' : {
		'defensiveness' : 0.8
	},
	'marine_brigade' : {
		'defensiveness' : 0.8
	},
	'paratrooper_brigade' : {
		'defensiveness' : 0.8,
	},
	'bergsjaeger_brigade' : {
		'defensiveness' : 0.8
	},
	'motorized_brigade' : {
		'defensiveness' : 0.8
	},
	'mechanized_brigade' : {
		'defensiveness' : 0.8
	},
	'alpini_brigade' : {
		'defensiveness' : 0.8
	},
	'alpins_brigade' : {
		'defensiveness' : 0.8
	},
	'gurkha_brigade' : {
		'defensiveness' : 0.8
	},
	'waffen_brigade' : {
		'defensiveness' : 0.8
	},
	'Guards_brigade' : {
		'defensiveness' : 0.8
	},
	'imperial_brigade' : {
		'defensiveness' : 0.8
	},
	'ranger_brigade' : {
		'defensiveness' : 0.8
	},
	'allow' : {
		'infantry_activation' : 1
	},
	'research_bonus_from' : {
		'infantry_theory' : 0.3,
		'infantry_practical' : 0.6,
		'superior_firepower_theory' : 0.1
	},
	'on_completion' : 'infantry_theory',
	'difficulty' : 0,
	#common for all techs.,
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936,
	'additional_offset' : 2,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'infantry_folder'
},

'infantry_guns' : {
	#each model gets this added to it.,
	'infantry_brigade' : {
		'toughness' : 0.6
	},
	'marine_brigade' : {
		'toughness' : 0.6
	},
	'paratrooper_brigade' : {
		'toughness' : 0.6
	},
	'bergsjaeger_brigade' : {
		'toughness' : 0.6
	},
	'motorized_brigade' : {
		'toughness' : 0.6
	},
	'mechanized_brigade': {
		'toughness' : 0.6
	},
	'alpini_brigade' : {
		'toughness' : 0.6
	},
	'alpins_brigade' : {
		'toughness' : 0.6
	},
	'gurkha_brigade' : {
		'toughness' : 0.6
	},
	'waffen_brigade' : {
		'toughness' : 0.6
	},
	'Guards_brigade' : {
		'toughness' : 0.6
	},
	'imperial_brigade' : {
		'toughness' : 0.6
	},
	'ranger_brigade' : {
		'toughness' : 0.6
	},
	'allow' : {
		'infantry_activation' : 1
	},
	'research_bonus_from' : {
		'infantry_theory' : 0.3,
		'infantry_practical' : 0.5,
		'artillery_practical' : 0.2
	},
	'on_completion' : 'infantry_theory',
	'difficulty' : 1,
	#common for all techs.,
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936,
	'additional_offset' : 2,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'infantry_folder'
},

'infantry_at' : {
	#each model gets this added to it.,
	'infantry_brigade' : {
		'hard_attack' : 0.25,
		'ap_attack' : 1
	},
	'marine_brigade' : {
		'hard_attack' : 0.2,
		'ap_attack' : 0.5
	},
	'paratrooper_brigade' : {
		'hard_attack' : 0.2,
		'ap_attack' : 0.5
	},
	'bergsjaeger_brigade' : {
		'hard_attack' : 0.2,
		'ap_attack' : 0.5
	},
	'motorized_brigade' : {
		'hard_attack' : 0.25,
		'ap_attack' : 1
	},
	'mechanized_brigade': {
		'hard_attack' : 0.25,
		'ap_attack' : 1
	},
	'alpini_brigade' : {
		'hard_attack' : 0.25,
		'ap_attack' : 1
	},
	'alpins_brigade' : {
		'hard_attack' : 0.25,
		'ap_attack' : 1
	},
	'gurkha_brigade' : {
		'hard_attack' : 0.25,
		'ap_attack' : 1
	},
	'waffen_brigade' : {
		'hard_attack' : 0.25,
		'ap_attack' : 1
	},
	'Guards_brigade' : {
		'hard_attack' : 0.25,
		'ap_attack' : 1
	},
	'imperial_brigade' : {
		'hard_attack' : 0.25,
		'ap_attack' : 1
	},
	'ranger_brigade' : {
		'hard_attack' : 0.25,
		'ap_attack' : 1
	},
	'allow' : {
		'infantry_activation' : 1
	},
	'research_bonus_from' : {
		'infantry_theory' : 0.3,
		'infantry_practical' : 0.6,
		'artillery_practical' : 0.1
	},
	'on_completion' : 'infantry_theory',
	'difficulty' : 1,
	#common for all techs.,
	'start_year' : 1918,
	'first_offset' : 1936,	#2nd model is from 1936,
	'additional_offset' : 2,	#one new every 2 years,
	'max_level' : 12,
	'folder' : 'infantry_folder'
},

'mountain_infantry' : {
	'activate_unit' : 'bergsjaeger_brigade',
	'activate_unit' : 'gurkha_brigade',
	'activate_unit' : 'alpins_brigade',
	'activate_unit' : 'alpini_brigade',
	'allow' : {
		'smallarms_technology' : 1,
		'infantry_support' : 1,
		'infantry_guns' : 1,
		'infantry_at' : 1,
	},
	'research_bonus_from' : {
		'infantry_theory' : 0.3,
		'infantry_practical' : 0.7
	},
	'on_completion' : 'infantry_theory',
	'difficulty' : 3,
	'start_year' : 1918,
	'folder' : 'infantry_folder'
},

'marine_infantry' : {
	'activate_unit' : 'marine_brigade' ,
	'activate_unit' : 'imperial_brigade',
	'allow' : {
		'smallarms_technology' : 2,
		'infantry_support' : 2,
		'infantry_guns' : 2,
		'infantry_at' : 2
	},
	'research_bonus_from' : {
		'infantry_theory' : 0.3,
		'infantry_practical' : 0.6,
		'land_doctrine_practical' : 0.1
	},
	'on_completion' : 'infantry_theory',
	'difficulty' : 3,
	'start_year' : 1937,
	'folder' : 'infantry_folder'
},

'paratrooper_infantry' : {
	'activate_unit' : 'paratrooper_brigade' ,
	'activate_unit' : 'ranger_brigade',
	'allow' : {
		'smallarms_technology' : 3,
		'infantry_support' : 3,
		'infantry_guns' : 3,
		'infantry_at' : 3
	},
	'research_bonus_from' : {
		'infantry_theory' : 0.3,
		'infantry_practical' : 0.6,
		'land_doctrine_practical' : 0.1
	},
	'on_completion' : 'infantry_theory',
	'difficulty' : 3,
	'start_year' : 1939,
	'folder' : 'infantry_folder'
},

'night_goggles' : {
	'infantry_brigade' : {
		'night' : { 
			'attack' : 0.1
			#'defence' : 0.3,
		}
	},
	'marine_brigade' : {
		'night' : { 
			'attack' : 0.1
			#'defence' : 0.3,
		}	
	},
	'paratrooper_brigade' : {
		'night' : { 
			'attack' : 0.1,
			#'defence' : 0.3,
		}
	},
	'bergsjaeger_brigade' : {
		'night' : { 
			'attack' : 0.1
			#'defence' : 0.3,
		}
	},
	'alpini_brigade' : {
		'night' : { 
			'attack' : 0.1
		}
	},
	'alpins_brigade' : {
		'night' : { 
			'attack' : 0.1
		}
	},
	'gurkha_brigade' : {
		'night' : { 
			'attack' : 0.1
		}
	},
	'waffen_brigade' : {
		'night' : { 
			'attack' : 0.1
		}
	},
	'Guards_brigade' : {
		'night' : { 
			'attack' : 0.1
		}
	},
	'imperial_brigade' : {
		'night' : { 
			'attack' : 0.1
		}
	},
	'ranger_brigade' : {
		'night' : { 
			'attack' : 0.1
		}
	},
	'allow' : {
		'radio_technology' : 1
	},
	'research_bonus_from' : {
		'infantry_theory' : 0.3,
		'infantry_practical' : 0.6,
		'land_doctrine_practical' : 0.1
	},
	'on_completion' : 'infantry_theory',
	'difficulty' : 10,
	'start_year' : 1944,
	'folder' : 'infantry_folder'
},

'engineer_brigade_activation' : {
	'activate_unit' : 'engineer_brigade',
	'allow' : {
		'industral_production' : 1
	},
	'research_bonus_from' : {
		'infantry_theory' : 0.3,
		'infantry_practical' : 0.6,
		'land_doctrine_practical' : 0.1
	},
	'on_completion' : 'infantry_theory',
	'difficulty' : 3,
	'start_year' : 1935,
	'folder' : 'infantry_folder'
},

'engineer_bridging_equipment' : {
	'engineer_brigade' : {
		'river' : {
			'attack' : 0.05,
			'movement' : 0.05
		}
	},
	'allow' : {
		'engineer_brigade_activation' : 1,
	},
	'research_bonus_from' : {
		'infantry_theory' : 0.3,
		'infantry_practical' : 0.6,
		'spearhead_theory' : 0.1
	},
	'on_completion' : 'infantry_theory',
	'difficulty' : 3,
	'start_year' : 1939,
	'first_offset' : 1940,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'infantry_folder'
},
'engineer_assault_equipment' : {
	'engineer_brigade' : {
     	   	'soft_attack' : 0.2,
        	'hard_attack' : 0.2,
        	'fort' : {
            		'attack' : 0.1
        	},
	        'urban' : {
        	    'attack' : 0.02
        	},
        	'jungle' : {
            		'attack' : 0.02
        	},
        	'forest' : {
            		'attack' : 0.01
        	}
    	},
	'allow' : {
		'engineer_brigade_activation' : 1
	},
	'research_bonus_from' : {
		'infantry_theory' : 0.3,
		'infantry_practical' : 0.6,
		'superior_firepower_theory' : 0.1
	},
	'on_completion' : 'infantry_theory',
	'difficulty' : 3,
	'start_year' : 1939,
	'first_offset' : 1940,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'infantry_folder'
},

'imporved_police_brigade' : {
	'police_brigade' : {
		'suppression' : 2
	},
	'research_bonus_from' : {
		'infantry_theory' : 0.3,
		'infantry_practical' : 0.7
	},
	'on_completion' : 'infantry_theory',
	'difficulty' : 1,
	'start_year' : 1939,
	'first_offset' : 1942,
	'additional_offset' : 3,
	'max_level' : 12,
	'folder' : 'infantry_folder'
},

'mortorised_infantry' : {
	'activate_unit' : 'motorized_brigade' ,
	'activate_unit' : 'mot_aa_brigade',
	'activate_unit' : 'waffenSS_brigade',
	'allow' : {
		'cavalry_smallarms' : 3,
		'cavalry_support' : 3,
		'cavalry_guns' : 3,
		'cavalry_at' : 3
	},
	'research_bonus_from' : {
		'mobile_theory' : 0.3,
		'mobile_practical' : 0.6,
		'armour_practical' : 0.1
	},
	'on_completion' : 'mobile_theory',
	'difficulty' : 3,
	'start_year' : 1938,
	'folder' : 'infantry_folder'
},

'desert_warfare_equipment' : {
	'infantry_brigade' : {
		'desert' : {
			'attrition' : -1
		}
	},
	'marine_brigade' : {
		'desert' : {
			'attrition' : -1
		}	,
	},
	'paratrooper_brigade' : {
		'desert' : {
			'attrition' : -1
		}
	},
	'bergsjaeger_brigade' : {
		'desert' : {
			'attrition' : -1
		}
	},
	'alpini_brigade' : {
		'desert' : {
			'attrition' : -1
		}
	},
	'alpins_brigade' : {
		'desert' : {
			'attrition' : -1
		}
	},
	'gurkha_brigade' : {
		'desert' : {
			'attrition' : -1
		}
	},
	'waffen_brigade' : {
		'desert' : {
			'attrition' : -1
		}
	},
	'Guards_brigade' : {
		'desert' : {
			'attrition' : -1
		}
	},
	'imperial_brigade' : {
		'desert' : {
			'attrition' : -1
		}
	},
	'ranger_brigade' : {
		'desert' : {
			'attrition' : -1
		}
	},
	'allow' : {
		'smallarms_technology' : 2,
		'infantry_support' : 2,
		'infantry_guns' : 2,
		'infantry_at' : 2
	},
	'research_bonus_from' : {
		'spearhead_theory' : 0.3,
		'land_doctrine_practical' : 0.7
	},
	'on_completion' : 'spearhead_theory',
	'difficulty' : 4,
	'start_year' : 1938,
	'folder' : 'infantry_folder'
},

'jungle_warfare_equipment' : {
	'infantry_brigade' : {
		'jungle' : {
			'attrition' : -1
		}
	},
	'marine_brigade' : {
		'jungle' : {
			'attrition' : -1
		}	
	},
	'paratrooper_brigade' : {
		'jungle' : {
			'attrition' : -1
		},
	},
	'bergsjaeger_brigade' : {
		'jungle' : {
			'attrition' : -1
		}
	},
	'alpini_brigade' : {
		'jungle' : {
			'attrition' : -1
		}
	},
	'alpins_brigade' : {
		'jungle' : {
			'attrition' : -1
		}
	},
	'gurkha_brigade' : {
		'jungle' : {
			'attrition' : -1
		}
	},
	'waffen_brigade' : {
		'jungle' : {
			'attrition' : -1
		}
	},
	'Guards_brigade' : {
		'jungle' : {
			'attrition' : -1
		}
	},
	'imperial_brigade' : {
		'jungle' : {
			'attrition' : -1
		}
	},
	'ranger_brigade' : {
		'jungle' : {
			'attrition' : -1
		}
	},
	'allow' : {
		'smallarms_technology' : 2,
		'infantry_support' : 2,
		'infantry_guns' : 2,
		'infantry_at' : 2
	},
	'research_bonus_from' : {
		'grand_battleplan_theory' : 0.3,
		'land_doctrine_practical' : 0.7
	},
	'on_completion' : 'grand_battleplan_theory',
	'difficulty' : 4,
	'start_year' : 1939,
	'folder' : 'infantry_folder'
},

'mountain_warfare_equipment' : {
	'infantry_brigade' : {
		'mountain' : {
			'attrition' : -1
		}
	},
	'marine_brigade' : {
		'mountain' : {
			'attrition' : -1
		}	
	},
	'paratrooper_brigade' : {
		'mountain' : {
			'attrition' : -1
		}
	},
	'bergsjaeger_brigade' : {
		'mountain' : {
	            'attrition' : -1.5,
	            'attack' : 0.1,
        	    'defence' : 0.1
		}
	},
	'alpini_brigade' : {
		'mountain' : {
	            'attrition' : -1.5,
	            'attack' : 0.1,
        	    'defence' : 0.1
		},
	},
	'alpins_brigade' : {
		'mountain' : {
	            'attrition' : -1.5,
	            'attack' : 0.1,
        	    'defence' : 0.1
		}
	},
	'gurkha_brigade' : {
		'mountain' : {
	            'attrition' : -1.5,
	            'attack' : 0.1,
        	    'defence' : 0.1
		}
	},
	'waffen_brigade' : {
		'mountain' : {
			'attrition' : -1
		}
	},
	'Guards_brigade' : {
		'mountain' : {
			'attrition' : -1
		}
	},
	'imperial_brigade' : {
		'mountain' : {
			'attrition' : -1
		}
	},
	'ranger_brigade' : {
		'mountain' : {
			'attrition' : -1
		}
	},
	'allow' : {
		'smallarms_technology' : 2,
		'infantry_support' : 2,
		'infantry_guns' : 2,
		'infantry_at' : 2
	},
	'research_bonus_from' : {
		'human_wave_theory' : 0.3,
		'land_doctrine_practical' : 0.7
	},
	'on_completion' : 'human_wave_theory',
	'difficulty' : 4,
	'start_year' : 1936,
	'folder' : 'infantry_folder'
},

'artic_warfare_equipment' : {
	'infantry_brigade' : {
		'arctic' : {
			'attrition' : -1
		}
	},
	'marine_brigade' : {
		'arctic' : {
			'attrition' : -1
		}	
	},
	'paratrooper_brigade' : {
		'arctic' : {
			'attrition' : -1
		}
	},
	'bergsjaeger_brigade' : {
		'arctic' : {
			'attrition' : -1
		}
	},
	'alpini_brigade' : {
		'arctic' : {
			'attrition' : -1
		}
	},
	'alpins_brigade' : {
		'arctic' : {
			'attrition' : -1
		}
	},
	'gurkha_brigade' : {
		'arctic' : {
			'attrition' : -1
		}
	},
	'waffen_brigade' : {
		'arctic' : {
			'attrition' : -1
		}
	},
	'Guards_brigade' : {
		'arctic' : {
			'attrition' : -1
		}
	},
	'imperial_brigade' : {
		'arctic' : {
			'attrition' : -1
		}
	},
	'ranger_brigade' : {
		'arctic' : {
			'attrition' : -1
		}
	},
	'allow' : {
		'smallarms_technology' : 2,
		'infantry_support' : 2,
		'infantry_guns' : 2,
		'infantry_at' : 2
	},
	'research_bonus_from' : {
		'human_wave_theory' : 0.3,
		'land_doctrine_practical' : 0.7
	},
	'on_completion' : 'human_wave_theory' ,
	'difficulty' : 4,
	'start_year' : 1936,
	'folder' : 'infantry_folder'
},

'amphibious_warfare_equipment' : {
	'marine_brigade' : {
		'amphibious' : {
			'attack' : 0.1
		}
	},
	'Guards_brigade' : {
		'amphibious' : {
			'attack' : 0.1
		}
	},
	'allow' : {
		'marine_infantry' : 1
	},
	'research_bonus_from' : {
		'superior_firepower_theory' : 0.3,
		'land_doctrine_practical' : 0.7
	},
	'on_completion' : 'superior_firepower_theory',
	'difficulty' : 7,
	'start_year' : 1940,
	'folder' : 'infantry_folder'
},

'airborne_warfare_equipment' : {
	'paratrooper_brigade' : {
		'transport_weight' : -1,
		'supply_consumption' : -0.1
	},
	'gurkha_brigade' : {
		'transport_weight' : -1,
		'supply_consumption' : -0.1
	},
	'ranger_brigade' : {
		'transport_weight' : -1,
		'supply_consumption' : -0.1
	},
	'allow' : {
		'paratrooper_infantry' : 1
	},
	'research_bonus_from' : {
		'grand_battleplan_theory' : 0.3,
		'land_doctrine_practical' : 0.7
	},
	'on_completion' : 'grand_battleplan_theory',
	'difficulty' : 7,
	'start_year' : 1941,
	'folder' : 'infantry_folder'
}
}

land_doctrine = {

'mobile_warfare' : {
    'hq_brigade' : {
        'maximum_speed' : 0.5
    },
   'tactic_breakthrough' : 5,
   'tactic_superior_breakthrough' : 2,
    'research_bonus_from' : {
        'spearhead_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'spearhead_theory',
    'difficulty' : 5,
    #common for all techs.
    'start_year' : 1918,
    'first_offset' : 1936, #2nd model is from 1936
    'additional_offset' : 2 ,  #one new every 2 years
    'max_level' : 12,
    'folder' : 'land_doctrine_folder'
},

'elastic_defence' : {
    'hq_brigade' : {
        'default_organisation' : 5
    },
	'sp_artillery_brigade' : {    
		'default_morale' : 0.05   
	},
	'sp_rct_artillery_brigade' : {    
		'default_morale' : 0.05   
	},
	'mot_aa_brigade' : {    
		'default_morale' : 0.05   
	},
    'tactic_tactical_withdrawal' : 3,
	'tactic_elastic_defence' : 3,
	'tactic_backhand_blow' : 1,
    'research_bonus_from' : {
        'spearhead_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'spearhead_theory',
    'difficulty' : 5,
    #common for all techs.
    'start_year' : 1918,
    'first_offset' : 1936, #2nd model is from 1936
    'additional_offset' : 2,   #one new every 2 years
    'max_level' : 12,
    'folder' : 'land_doctrine_folder'
},

'spearhead_doctrine' : {
    'armor_brigade' : {
        'combat_width' : -1
    },
    'heavy_armor_brigade' : {
        'combat_width' : -1
    },
    'light_armor_brigade' : {
        'combat_width' : -1
    },
    'allow' : {
        'mobile_warfare' : 3
        # 'land_doctrine_practical' : 10
    },
    'research_bonus_from' : {
        'spearhead_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'spearhead_theory',
    'difficulty' : 5,
    'start_year' : 1940,
    'folder' : 'land_doctrine_folder'
},

'schwerpunkt' : {
    'armor_brigade' : {       'default_morale' : 0.05   },
    'heavy_armor_brigade' : {     'default_morale' : 0.05   },
    'light_armor_brigade' : {     'default_morale' : 0.05   },
    'super_heavy_armor_brigade' : {   'default_morale' : 0.05   },
    'tank_destroyer_brigade'  : { 'default_morale' : 0.05   },
    'armored_car_brigade' : {     'default_morale' : 0.05   },
    'research_bonus_from' : {
        'spearhead_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'spearhead_theory',
    'difficulty' : 10,
    #common for all techs.
    'start_year' : 1918,
    'first_offset' : 1936, #2nd model is from 1936
    'additional_offset' : 2 ,  #one new every 2 years
    'max_level' : 12,
    'folder' : 'land_doctrine_folder'
},

'blitzkrieg' : {
    'armor_brigade' : {       'default_organisation' : 5    },
    'heavy_armor_brigade' : {     'default_organisation' : 5    },
    'light_armor_brigade' : {     'default_organisation' : 5    },
    'super_heavy_armor_brigade' : {   'default_organisation' : 5    },
    'tank_destroyer_brigade'  : { 'default_organisation' : 5    },
    'armored_car_brigade' : {     'default_organisation' : 5    },
	'mot_aa_brigade' : {    'default_organisation' : 5    },
	'tactic_blitz' : 3,
	'tactic_super_blitz' : 1,
    'research_bonus_from' : {
        'spearhead_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'spearhead_theory',
    'difficulty' : 10,
    #common for all techs.
    'start_year' : 1918,
    'first_offset' : 1936, #2nd model is from 1936
    'additional_offset' : 2,   #one new every 2 years
    'max_level' : 12,
    'folder' : 'land_doctrine_folder'
},

'operational_level_command_structure' : {
    'attack_movement_speed' : 0.02,
    'research_bonus_from' : {
        'spearhead_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'spearhead_theory',
    'difficulty' : 5,
    #common for all techs.
    'start_year' : 1918,
    'first_offset' : 1936, #2nd model is from 1936
    'additional_offset' : 2 ,  #one new every 2 years
    'max_level' : 12,
    'folder' : 'land_doctrine_folder'
},

'tactical_command_structure' : {
    'mechanized_brigade' : {      'default_morale' : 0.05   },
    'motorized_brigade' : {       'default_morale' : 0.05   },
    'cavalry_brigade' : {     'default_morale' : 0.05   },
	'waffen_brigade' : {       'default_morale' : 0.05   },
    'research_bonus_from' : {
        'superior_firepower_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'superior_firepower_theory',
    'difficulty' : 5,
    #common for all techs.
    'start_year' : 1918,
    'first_offset' : 1937, #2nd model is from 1936
    'additional_offset' : 2 ,  #one new every 2 years
    'max_level' : 12,
    'folder' : 'land_doctrine_folder'
},

'delay_doctrine' : {
    'hq_brigade' : {
        'soft_attack' : 0.1
    },
	'anti_air_brigade' : {      'default_morale' : 0.05   },
	'anti_tank_brigade' : {      'default_morale' : 0.05   },
	'artillery_brigade' : {      'default_morale' : 0.05   },
	'rocket_artillery_brigade' : {      'default_morale' : 0.05   },
    'tactic_delay' : 3,
	'tactic_slower_delay' : 1,
    'research_bonus_from' : {
        'superior_firepower_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'superior_firepower_theory',
    'difficulty' : 5,
    #common for all techs.
    'start_year' : 1918,
    'first_offset' : 1936, #2nd model is from 1936
    'additional_offset' : 2 ,  #one new every 2 years
    'max_level' : 12,
    'folder' : 'land_doctrine_folder'
},

'integrated_support_doctrine' : {
    'hq_brigade' : {
        'hard_attack' : 0.1
    },
	'paratrooper_brigade' : {
		'default_morale' : 0.05
			},
	'marine_brigade' : {
		'default_morale' : 0.05
			},
	'bergsjaeger_brigade' : {
		'default_morale' : 0.05
			},
	'alpini_brigade' : {
		'default_morale' : 0.05
	},
	'alpins_brigade' : {
		'default_morale' : 0.05
	},
	'gurkha_brigade' : {
		'default_morale' : 0.05
	},
	'guards_brigade' : {
		'default_morale' : 0.05
	},
	'imperial_brigade' : {
		'default_morale' : 0.05
	},
	'ranger_brigade' : {
		'default_morale' : 0.05
	},
    'tactic_encirclement' : 3,
    'research_bonus_from' : {
        'superior_firepower_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'superior_firepower_theory',
    'difficulty' : 5,
    #common for all techs.
    'start_year' : 1918,
    'first_offset' : 1936, #2nd model is from 1936
    'additional_offset' : 2 ,  #one new every 2 years
    'max_level' : 12,
    'folder' : 'land_doctrine_folder'
},

'superior_firepower' : {
    'division_size' : 1 ,  #'allow' 1 extra brigade
    'allow' : {
        'tactical_command_structure' : 3
        # 'land_doctrine_practical' : 10
    },
    'research_bonus_from' : {
        'superior_firepower_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'superior_firepower_theory',
    'difficulty' : 5,
    'start_year' : 1940,
    'folder' : 'land_doctrine_folder'
},

'mechanized_offensive' : {
    'mechanized_brigade' : {      'default_organisation' : 5    },
    'motorized_brigade' : {       'default_organisation' : 5    },
    'cavalry_brigade' : {         'default_organisation' : 5    },
	'waffen_brigade' : {       'default_organisation' : 5    },
    'research_bonus_from' : {
        'superior_firepower_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'superior_firepower_theory',
    'difficulty' : 10,
    #common for all techs.
    'start_year' : 1935,
    'first_offset' : 1937, #2nd model is from 1936
    'additional_offset' : 2 ,  #one new every 2 years
    'max_level' : 12,
    'folder' : 'land_doctrine_folder'
},

'combined_arms_warfare' : {
    # combined_arms_bonus : 0.1
    'armor_unit_type' : 0.1,
	'activate_unit' : 'sp_rct_artillery_brigade' ,
    'research_bonus_from' : {
        'superior_firepower_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'allow' : {
        'mechanized_offensive' : 3
    },
    'change' : 'no',
    'on_completion' : 'superior_firepower_theory',
    'difficulty' : 10,
    #common for all techs.
    'start_year' : 1940,
    'folder' : 'land_doctrine_folder'
},

'infantry_warfare' : {
    'infantry_brigade' : {        'default_organisation' : 5    },
    'research_bonus_from' : {
        'grand_battleplan_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'grand_battleplan_theory',
    'difficulty' : 5,
    #common for all techs.
    'start_year' : 1918,
    'first_offset' : 1936 ,#2nd model is from 1936
    'additional_offset' : 2 ,  #one new every 2 years
    'max_level' : 12,
    'folder' : 'land_doctrine_folder'
},

'special_forces' : {
    'paratrooper_brigade' : {     'default_organisation' : 5    },
    'bergsjaeger_brigade' : {     'default_organisation' : 5    },
    'marine_brigade' : {      'default_organisation' : 5    },
	'engineer_brigade' : {        
		'default_organisation' : 5    
	},
	'alpini_brigade' : {
		'default_organisation' : 5
	},
	'alpins_brigade' : {
		'default_organisation' : 5
	},
	'gurkha_brigade' : {
		'default_organisation' : 5
	},
	'guards_brigade' : {
		'default_organisation' : 5
	},
	'imperial_brigade' : {
		'default_organisation' : 5
	},
	'ranger_brigade' : {
		'default_organisation' : 5
	},
    'research_bonus_from' : {
        'grand_battleplan_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'grand_battleplan_theory',
    'difficulty' : 5,
    #common for all techs.
    'start_year' : 1918,
    'first_offset' : 1936 ,#2nd model is from 1936
    'additional_offset' : 2 ,  #one new every 2 years
    'max_level' : 12,
    'folder' : 'land_doctrine_folder'
},

'central_planning' : {
    'hq_brigade' : {
        'toughness' : 1
    },
    'tactic_counterattack' : 3,
    'research_bonus_from' : {
        'grand_battleplan_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'grand_battleplan_theory',
    'difficulty' : 5,
    #common for all techs.
    'start_year' : 1918,
    'first_offset' : 1936, #2nd model is from 1936
    'additional_offset' : 2 ,  #one new every 2 years
    'max_level' : 12,
    'folder' : 'land_doctrine_folder'
},

'mass_assault' : {
    'tactic_assault' : 3,
	'tactic_reckless_assault' : 1,
    'hq_brigade' : {
        'defensiveness' : 1
    },
		'infantry_brigade' : {
		'default_morale' : 0.05
			},
    'research_bonus_from' : {
        'grand_battleplan_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'grand_battleplan_theory',
    'difficulty' : 5,
    #common for all techs.
    'start_year' : 1918,
    'first_offset' : 1936 ,#2nd model is from 1936
    'additional_offset' : 2 ,  #one new every 2 years
    'max_level' : 12,
    'folder' : 'land_doctrine_folder'
},

'grand_battle_plan' : {
    'reinforce_chance' : 0.05,
    'research_bonus_from' : {
        'grand_battleplan_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'allow' : {
        'central_planning' : 3
    },
    'change' : 'no',
    'on_completion' : 'grand_battleplan_theory',
    'difficulty' : 5,
    #common for all techs.
    'start_year' : 1940,
    'folder' : 'land_doctrine_folder'
},

'assault_concentration' : {
    'anti_air_brigade' : {        'default_organisation' : 5    },
    'anti_tank_brigade' : {       'default_organisation' : 5    },
    'sp_artillery_brigade' : {    'default_organisation' : 5    },
    'sp_rct_artillery_brigade' : {    'default_organisation' : 5    },
    'rocket_artillery_brigade' : {    'default_organisation' : 5    },
    'artillery_brigade' : {       'default_organisation' : 5    },
    'research_bonus_from' : {
        'grand_battleplan_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'grand_battleplan_theory',
    'difficulty' : 5,
    #common for all techs.
    'start_year' : 1918,
    'first_offset' : 1936, #2nd model is from 1936
    'additional_offset' : 2 ,  #one new every 2 years
    'max_level' : 12,
    'folder' : 'land_doctrine_folder'
},

'operational_level_organisation' : {
    'attack_delay' : 24,
    'research_bonus_from' : {
        'human_wave_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'human_wave_theory',
    'difficulty' : 4,
    #common for all techs.
    'start_year' : 1918,
    'first_offset' : 1936,
    'additional_offset' : 4,
    'max_level' : 12,
    'folder' : 'land_doctrine_folder'
},

'large_front' : {
    'hq_brigade' : {
        'supply_consumption' : -0.01
    },
	'militia_brigade' : {
		'default_morale' : 0.05
			},
	'garrison_brigade' : {
		'default_morale' : 0.02
			},
    'tactic_shock' : 3,
    'research_bonus_from' : {
        'human_wave_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'human_wave_theory',
    'difficulty' : 5,
    #common for all techs.
    'start_year' : 1918,
    'first_offset' : 1936, #2nd model is from 1936
    'additional_offset' : 2 ,  #one new every 2 years
    'max_level' : 12,
    'folder' : 'land_doctrine_folder'
},

'guerilla_warfare' : {
    'hq_brigade' : {
        'supply_consumption' : -0.01
    },
    'tactic_ambush' : 3,
    'research_bonus_from' : {
        'human_wave_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'human_wave_theory',
    'difficulty' : 5,
    #common for all techs.
    'start_year' : 1918,
    'first_offset' : 1936, #2nd model is from 1936
    'additional_offset' : 2 ,  #one new every 2 years
    'max_level' : 12,
    'folder' : 'land_doctrine_folder'
},

'peoples_army' : {
    'militia_brigade' : {     'default_organisation' : 5    },
    'partisan_brigade' : {        'default_organisation' : 5    },
    'garrison_brigade' : {        'default_organisation' : 5    },
    'police_brigade' : {      'default_organisation' : 5    },
    'research_bonus_from' : {
        'human_wave_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'human_wave_theory',
    'difficulty' : 3,
    #common for all techs.
    'start_year' : 1918,
    'first_offset' : 1936 ,#2nd model is from 1936
    'additional_offset' : 2 ,  #one new every 2 years
    'max_level' : 12,
    'folder' : 'land_doctrine_folder'
},

'large_formations' : {
    'militia_brigade' : { 'combat_width' : -0.5 },
    'partisan_brigade' : { 'combat_width' : -0.5 },
    'allow' : {
        'large_front' : 3,
        'peoples_army' : 3
    },
    'change' : 'no',
    'research_bonus_from' : {
        'human_wave_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'on_completion' : 'human_wave_theory',
    'difficulty' : 3,
    #common for all techs.
    'start_year' : 1939,
    'folder' : 'land_doctrine_folder'
},

'human_wave' : {
    'unit_cooperation' : 0.05,
    'research_bonus_from' : {
        'human_wave_theory' : 0.3,
        'land_doctrine_practical' : 0.7
    },
    'change' : 'no',
    'on_completion' : 'human_wave_theory',
    'difficulty' : 5,
    'allow' : {
        'large_front' : 3
    },
    #common for all techs.
    'start_year' : 1940,
    'folder' : 'land_doctrine_folder'
}
}
laws = {
'civil_law' : {
    'open_society' : {
    	'cost' : 0.5,
        'counter_intelligence' : -0.25
    },

    'limited_restrictions' : {
    	'cost' : 0.5,
        'war_consumer_goods_demand' : -0.01,
        'counter_intelligence' : -0.10,
        'partisan_efficiency' : 1
    },

    'legalistic_restrictions' : {
    	'cost' : 0.5,
        'war_consumer_goods_demand' : -0.02,
        'partisan_efficiency' : 2
    },

    'repression' : {
    	'cost' : 0.5,
        'war_consumer_goods_demand' : -0.03,
        'counter_intelligence' : 0.1,
        'partisan_efficiency' : 3,
        'ruling_party_support' : 0.1
    },

    'totalitarian_system' : {
    	'cost' : 0.5,
        'war_consumer_goods_demand' : -0.04,
        'counter_intelligence' : 0.25,
        'partisan_efficiency' : 4,
        'ruling_party_support' : 0.3
    }
},

'conscription_law' : {
    'volunteer_army' : {
    	'cost' : 0.5,
        'reserves_penalty_size' : -0.75,
        'global_manpower_modifier' : -0.50,
        'peacetime_manpower_rotation' : 0.10,
	'officer_recruitment' : -0.50
    },

    'one_year_draft' : {
    	'cost' : 0.5,
        'reserves_penalty_size' : -0.66,
        'global_manpower_modifier' : -0.25,
        'peacetime_manpower_rotation' : 0.05,
        'officer_recruitment' : -0.25
    },
    'two_year_draft' : {
    	'cost' : 0.5,
        'reserves_penalty_size' : -0.5,
        'peacetime_manpower_rotation' : 0.03,
        'officer_recruitment' : 0
    },
    'three_year_draft' : {
    	'cost' : 0.5,
        'reserves_penalty_size' : -0.25,
        'global_manpower_modifier' : 0.25,
        'peacetime_manpower_rotation' : 0.02,
        'officer_recruitment' : 0.25
    },

    'service_by_requirement' : {
    	'cost' : 0.5,
        'reserves_penalty_size' : -0.10,
        'global_manpower_modifier' : 1.0,
        'peacetime_manpower_rotation' : 0.01,
        'officer_recruitment' : 0.5
    }
},

'economic_law' : {
    'full_civilian_economy' : {
    	'cost' : 0.5,
        'peace_consumer_goods_demand' : -0.1,
        'global_ic' : -0.5,
        'global_money' : 0.15  ,      
        'global_resources' : -0.5
    },

    'basic_mobilisation' : {
    	'cost' : 0.5,
	'peace_consumer_goods_demand' : -0.05,
        'global_ic' : -0.25,
        'global_resources' : -0.25,
        'global_money' : 0.05
    },

    'full_mobilisation' : {
    	'cost' : 0.5,
         'global_money' : -0.10
    },

    'war_economy' : {
    	'cost' : 0.5,
        'war_consumer_goods_demand' : -0.02,
        'global_ic' : 0.25 ,   
		'global_resources' : 0.10	,
        'global_money' : -0.25
    },

    'total_economic_mobilisation' : {
    	'cost' : 0.5,
        'war_consumer_goods_demand' : -0.05,
        'global_ic' : 0.50,
		'global_resources' : 0.25,
        'global_money' : -0.50
    }
},

'education_investment_law' : {
    'minimal_education_investment' : {
    	'cost' : 0.5,
        'global_money' : 0.10,
        'global_leadership_modifier' : -0.15
    },
    'average_education_investment' : {
    	'cost' : 0.5,
    },
    'medium_large_education_investment' : {
    	'cost' : 0.5,
        'global_money' : -0.10,
        'global_leadership_modifier' : 0.10
    },
    'big_education_investment': {
    	'cost' : 0.5,
        'global_money' : -0.25,
        'global_leadership_modifier' : 0.20
    }
},


'industrial_policy_laws' : {
    'consumer_product_orientation' : {
    	'cost' : 0.5,
        'peace_consumer_goods_demand' : -0.01,
        'industrial_efficiency' : -0.15, #5
        'dissent' : -0.2
    },

    'mixed_industry' : {
    	'cost' : 0.5,
        'war_consumer_goods_demand' : -0.05,
        'peace_consumer_goods_demand' : -0.05,
        'supply_throughput' : 0.05  ,  
		'global_supplies' : 0.1 #new		
    },

    'heavy_industry_emphasis' : {
    	'cost' : 0.5,
        'industrial_efficiency' : 0.05,
        'peace_consumer_goods_demand' : 0.1,
        'supply_throughput' : 0.1
    }
},

'press_laws' : {
    'free_press' : {
    	'cost' : 0.5,
        'national_unity_effect' : 0.2, #national unity effects are exagerated 
        'counter_espionage' : -0.10,
        'drift_speed' : 0.1
    },

    'censored_press' : {
    	'cost' : 0.5,
        'drift_speed' : 0.05
    },

    'state_press' : {
    	'cost' : 0.5,
        'national_unity_effect' : -0.05, #national unity effects are limited
        'drift_speed' : -0.05,
        'counter_espionage' : 0.05
    },

    'propaganda_press' : {
    	'cost' : 0.5,
        'national_unity_effect' : -0.1, #national unity effects are limited
        'counter_espionage' : 0.10,
        'drift_speed' : -0.1
    }
},


'training_laws' : {
    'minimal_training' : {
    	'cost' : 0.5,
        'unit_recruitment_time' : -0.10
    },

    'basic_training' : {
    	'cost' : 0.5,
        'unit_start_experience' : 10
    },

    'advanced_training' : {
    	'cost' : 0.5,
        'unit_recruitment_time' : 0.10,
        'unit_start_experience' : 15
    },

    'specialist_training' : {
    	'cost' : 0.5,
        'unit_recruitment_time' : 0.20,
        'unit_start_experience' : 25
    }
}
}
minsiters = {

'biased_intellectual' : {
	'suseptibility_comintern' : 0.10
}, 

'ideological_crusader' : {
	'drift_speed' : 0.05
}, 

'apologetic_clerk' : {
	'drift_speed' : -0.05
}, 

'iron_fisted_brute' : {
	'threat_impact' : 0.05,
	'ruling_party_support' : 0.1
}, 
'great_compromiser' : {
	'suseptibility_axis' : 0.1
}, 

'general_staffer' : {
	'peace_offmap_intel' : 0.1
}, 

'the_cloak_n_dagger_schemer' : {
	'suseptibility_allies' : 0.1
}, 

'administrative_genius' : {
	'global_ic' : 0.1
}, 

'resource_industrialist' : {
	'global_resources' : 0.05 ,
	'decay' : { 'chemical_engineering' : -0.25 }
}, 

'laissez_faires_capitalist' : {
	'war_consumer_goods_demand' : -0.025,
	'peace_consumer_goods_demand' : -0.025 
}, 

'theoretical_scientist' : {
	'decay' : { 'nuclear_physics' : -0.25 },
	'decay' : { 'jetengine_theory' : -0.25 }
}, 
'military_entrepreneur' : {
	'global_supplies' : 0.2
}, 

'battle_fleet_proponent' : {
	'decay' : { 'naval_engineering' : -0.25 }
}, 

'submarine_proponent' : {
	'decay' : { 'submarine_engineering' : -0.25 }
}, 

'tank_proponent' : {
	'decay' : { 'automotive_theory' : -0.25 },
	'decay' : { 'mobile_theory' : -0.25 }
}, 

'infantry_proponent' : {
	'decay' : { 'infantry_theory' : -0.25 },
	'decay' : { 'militia_theory' : -0.25 }
}, 

'air_to_ground_proponent' : {
	'decay' : { 'aeronautic_engineering' : -0.20 },
	'decay' : { 'single_engine_aircraft_practical' : -0.05 }
}, 

'air_to_sea_proponent' : {
	'decay' : { 'naval_engineering' : -0.20 },
	'decay' : { 'twin_engine_aircraft_practical' : -0.05 }
}, 

'strategic_air_proponent' : {
	'decay' : { 'aeronautic_engineering' : -0.20 },
	'decay' : { 'four_engine_aircraft_practical' : -0.05 }
}, 

'silent_lawyer' : {
	'threat_impact' : -0.1
},

'compassionate_gentleman' : {
	'national_unity_effect' : 0.1
}, 

'crime_fighter' : {
	'counter_espionage' : 0.1
}, 

'prince_of_terror' : {
	'ruling_party_support' : 0.15,
	'partisan_efficiency' : 10
}, 

'back_stabber' : {
	'dissent' : 0.01,
	'counter_espionage' : 0.1	
}, 

'man_of_the_people' : {
	'global_leadership_modifier' : 0.05
}, 

'efficient_sociopath' : {
	'counter_intelligence' : 0.1
}, 

'technical_specialist' : {
	'espionage_bonus' : 0.05
}, 

'research_specialist' : {
	'offmap_land_intel' : 0.2
}, 

'political_specialist' : {
	'offmap_political_intel' : 0.2
}, 

'dismal_enigma' : {
	'offmap_land_intel' : 0.1,
	'offmap_naval_intel' : 0.1
}, 

'industrial_specialist' : {
	'offmap_industry_intel' : 0.2
}, 

'naval_intelligence_specialist' : {
	'offmap_naval_intel' : 0.2
}, 

'school_of_manoeuvre' : {
	'combat_movement_speed' : 0.1
}, 

'school_of_fire_support' : {
	'attack_reinforce_chance' : 0.1
}, 

'school_of_mass_combat' : {
	'decay' : { 'human_wave_theory' : -0.10 },
	'global_manpower_modifier' : 0.05	
}, 

'school_of_psychology' : {
	'org_regain' : 0.1
}, 

'school_of_defence' : {
	'defend_reinforce_chance' : 0.1
}, 

'logistics_specialist' : {
	'supply_throughput' : 0.1
},

'elastic_defence_doctrine' : {
	'decay' : { 'mobile_practical' : -0.25 }
}, 

'static_defence_doctrine' : {
	'decay' : { 'infantry_practical' : -0.25 },
	'decay' : { 'militia_practical' : -0.25 }
}, 

'decisive_battle_doctrine' : {
	'decay' : { 'artillery_practical' : -0.25 }
}, 

'armoured_spearhead_doctrine' : {
	'decay' : { 'armour_practical' : -0.25 }
}, 

'guns_and_butter_doctrine' : {
	'supply_consumption' : -0.10
}, 

'open_seas_doctrine' : {
	'decay' : { 'destroyer_practical' : -0.25 }
}, 

'decisive_naval_battle_doctrine' : { 
	'decay' : { 'capitalship_practical' : -0.25 }
}, 

'power_projection_doctrine' : {
	'decay' : { 'carrier_practical' : -0.25 }
}, 

'indirect_approach_doctrine' : {
	'decay' : { 'cruiser_practical' : -0.25 }
}, 

'base_control_doctrine' : {
	'naval_base_efficiency' : 0.1
}, 

'air_superiority_doctrine' : {
	'decay' : { 'single_engine_aircraft_practical' : -0.25 }
}, 

'naval_aviation_doctrine' : {
	'decay' : { 'twin_engine_aircraft_practical' : -0.15 },
	'decay' : { 'naval_engineering' : -0.1 }
}, 

'army_aviation_doctrine' : {
	'decay' : { 'twin_engine_aircraft_practical' : -0.25 }
}, 

'carpet_bombing_doctrine' : {
	'decay' : { 'four_engine_aircraft_practical' : -0.25 }
}, 

'vertical_envelopment_doctrine' : {
	'decay' : { 'strategic_air_focus' : -0.10 }, 
}, 

'undistinguished_suit' : {
	'ruling_party_support' : 0.05
}, 

'air_superiority_proponent' : {
	'decay' : { 'fighter_focus' : -0.10 } 
}, 

'corrupt_kleptocrat' : {
	'global_supplies' : -0.1,
	'ruling_party_support' : 0.1
}, 

'crooked_kleptocrat' : {
	'global_ic' : -0.03,
	'ruling_party_support' : 0.1
}, 

'power_hungry_demagogue' : {
	'national_unity_effect' : -0.1,
	'espionage_bonus' : 0.05
}, 

'barking_buffoon' : {
	'national_unity_effect' : 0.1,
	'peace_offmap_intel' : 0.05
}, 

'stern_imperialist' : {
	'global_ic' : 0.05
}, 

'ruthless_powermonger' : {
	'land_organisation' : -0.05,
	'national_unity_effect' : -0.2
}, 

'autocratic_charmer' : {
	'global_ic' : -0.05,
	'war_consumer_goods_demand' : -0.5
}, 

'resigned_generalissimo' : {
	'global_supplies' : 0.1,
	'global_money' : -0.1
}, 

'benevolent_gentleman' : {
	'suseptibility' : -0.05,
	'org_regain' : 0.05
}, 

'weary_stiff_neck' : {
	'suseptibility' : 0.05,
	'org_regain' : 0.05
}, 

'insignificant_layman' : {
	'global_money' : 0.05,
	'war_consumer_goods_demand' : 0.025 
}, 

'die_hard_reformer' : {
	'industrial_efficiency' : 0.03
}, 
'pig_headed_isolationist' : {
	'suseptibility' : -0.5,
	'threat_resistance' : 0.1
}, 

'popular_figurehead' : {
	'ruling_party_support' : 0.1
},

'silent_workhorse' : {
	'global_ic' : 0.05
}, 
'naive_optimist' : {
	'threat_impact' : -0.1
}, 
'flamboyant_tough_guy' : {
	'drift_speed' : 0.05
}, 

'happy_amateur' : {
	'global_money' : -0.05,
	'peace_consumer_goods_demand' : -0.02
}, 
'backroom_backstabber' : {
	'global_ic' : -0.05,
	'ruling_party_support' : 0.1
}, 

'smiling_oilman' : {
	'global_crude_oil' : 0.05
}, 

'old_general' : {
	'land_organisation' : 0.05
}, 

'old_admiral' : {
	'naval_organisation' : 0.1	
}, 

'old_air_marshal' : {
	'air_organisation' : 0.1		
}, 

'political_protege' : {
	'ruling_party_support' : 0.05,
	'peace_consumer_goods_demand' : -0.025 	
}, 
'ambitious_union_boss' : {
	'war_consumer_goods_demand' : 0.05,
	'dissent' : -0.05	
}, 
'corporate_suit' : {
	'global_money' : 0.1
}
}
static_modifiers = {

'very_easy_player' : {
	'global_manpower_modifier' : 0.5,
	'global_revolt_risk' : -3,
	'global_ic' : 0.50,
	'global_resources' : 0.5,
	'supply_consumption' : -0.25,
	'supply_throughput' : 1.0,
	'naval_base_efficiency' : 1.0
},

'easy_player' : {
	'global_manpower_modifier' : 0.25,
	'global_revolt_risk' : -1,
	'global_ic' : 0.25,
	'global_resources' : 0.25,
	'supply_throughput' : 0.25,
	'naval_base_efficiency' : 0.25
},

'hard_player' : {
	'global_ic' : -0.1,
	'global_resources' : -0.1,
	'supply_throughput' : -0.25,
	'naval_base_efficiency' : -0.25
},

'very_hard_player' : {
	'global_ic' : -0.25,
	'global_resources' : -0.25,
	'supply_throughput' : -0.5,
	'naval_base_efficiency' : -0.5
},

'very_easy_ai' : {
	'global_ic' : -0.25,
	'global_resources' : -0.25
},

'easy_ai' : {
},

'hard_ai' : {
	'global_ic' : 0.25,
	'global_resources' : 0.25,
	'supply_consumption' : -0.25,
	'supply_throughput' : 0.25,
	'naval_base_efficiency' : 0.25
},

'very_hard_ai' : {
	'global_ic' : 1.0,
	'global_resources' : 1.0,
	'supply_consumption' : -0.5,
	'supply_throughput' : 0.5,
	'naval_base_efficiency' : 0.5

},

'overseas' : {
	'local_manpower_modifier' : -0.75	,	#75% penalty on manpower.
	'local_leadership_modifier' : -0.9	#75% on leadership.
},

'coastal' : {
},

'non_coastal' : {
},

'coastal_sea' : {
},

'sea_zone' : {
},

'land_province' : {
	'attrition' : 1
},

'blockaded' : 
{
},

'no_adjacent_controlled' : {
},

'non_core' : {
	'local_manpower_modifier' : -0.75,
	'local_leadership_modifier' : -0.80,
	'local_ic' : -0.50,
	'local_resources' : -0.5
},

'occupied' : {
	'local_revolt_risk' : 2			#2% revolt risk!
},

'revolt_risk' : {
	'local_ic' : -0.02,
	'local_manpower_modifier' : -0.02,
	'local_resources' : -0.02
},

'nationalism' : {
	'local_revolt_risk' : 0.3			#0.3% for each year revolt risk!
	#minimum_revolt_risk : 0.3		#0.3% for each year revolt risk!
},

'manpower' : {
},

'dissent' : {
	'global_ic' : -0.01
},

'neutrality' : {
	'drift_speed' : 0.05
},

'base_values' : {
	'war_consumer_goods_demand' : 0.15,
	'peace_consumer_goods_demand' : 0.25,
	'global_manpower' : 4,
	'global_leadership' : 3.5,
	'ic' : 5
},

'war' : {
	'max_war_exhaustion' : 100,			#base max at war.
	'war_exhaustion' : 0.041			#about 1/24 increase each month
},

'peace' : {
	'max_war_exhaustion' : 100			#base max at peace.
},

'war_exhaustion' : {
},

'luck' : {
},

'badboy' : {
},

'prestige' : {
},

'land_maintenance' : {
},

'naval_maintenance' : {
},

'initial_mobilization' : {
	'reinforcement_bonus' : 1.0	# each reinforcement produced also produces one bonus reinforcement
},

'government_in_exile' : {
},

'fractured_government' : {
	'national_unity' : -0.05
},

'align_towards_axis' : {
	'align_towards' : 20.0
} ,

'align_towards_allies' : {
	'align_towards' : 20.0
},

'align_towards_comintern' : {
	'align_towards' : 20.0
},

'spy_lower_national_unity' : {
	'national_unity' : -0.1
},

'spy_raise_national_unity' : {
	'national_unity' : 0.15
},

'disrupt_production' : {
	'unit_repair' : -0.02
},

'disrupt_research' : {
	'research_efficiency' : -0.01
},

'spy_lower_neutrality' : {
	'neutrality_change' : -0.003
},

'spy_support_resistance' : {
	'partisan_efficiency' : 5
},

'covertopsmod_sabotage' : {
	'local_unit_speed' : -0.2
},

'covertopsmod_scout' : {
	'local_unit_speed' : 0.2	
},

'covertopsmod_damage_resource' : {
	'strategic_resource_efficiency' : -0.99
}
}
naval_doctrine = {

'fleet_auxiliary_carrier_doctrine' : {
	'cag' : {
		'default_organisation' : 5 		
	},
	'research_bonus_from' : {
		'naval_doctrine_practical' : 0.7,
		'base_strike_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'base_strike_doctrine',
	'difficulty' : 5,
	'start_year' : 1936,
	'folder' : 'naval_doctrine_folder'
},

'light_cruiser_escort_role' : {
	'light_cruiser' : { 'positioning' : 0.05 },
	'allow' : {
		'fleet_auxiliary_carrier_doctrine' : 1
	},
	'research_bonus_from' : {
		'cruiser_practical' : 0.3,
		'naval_doctrine_practical' : 0.4,
		'base_strike_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'base_strike_doctrine' ,
	'difficulty' : 5,
	'start_year' : 1937,
	'first_offset' : 1939,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'naval_doctrine_folder'
},

'carrier_group_doctrine' : {
	'carrier' : { 'positioning' : 0.05 },
	'escort_carrier' : { 'positioning' : 0.05 },
	'allow' : {
		'fleet_auxiliary_carrier_doctrine' : 1
	},
	'research_bonus_from' : {
		'carrier_practical' : 0.3,
		'naval_doctrine_practical' : 0.4,
		'base_strike_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'base_strike_doctrine' ,
	'difficulty' : 5,
	'start_year' : 1937,
	'first_offset' : 1939,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'naval_doctrine_folder'
},

'light_cruiser_crew_training' : {
	'light_cruiser' : { 
		'default_organisation' : 5 ,
		'default_morale' : 0.05
	},
	'allow' : {
		'fleet_auxiliary_carrier_doctrine' : 1
	},
	'research_bonus_from' : {
		'cruiser_practical' : 0.3,
		'naval_doctrine_practical' : 0.4,
		'base_strike_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'base_strike_doctrine', 
	'difficulty' : 5,
	'start_year' : 1937,
	'first_offset' : 1939,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'naval_doctrine_folder'
},

'carrier_crew_training' : {
	'carrier' : {
		'default_organisation' : 5 ,
		'default_morale' : 0.05
	},
	'escort_carrier' : {
		'default_organisation' : 5 ,
		'default_morale' : 0.05
	},
	'allow' : {
		'fleet_auxiliary_carrier_doctrine' : 1
	},
	'research_bonus_from' : {
		'carrier_practical' : 0.3,
		'naval_doctrine_practical' : 0.4,
		'base_strike_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'base_strike_doctrine' ,
	'difficulty' : 5,
	'start_year' : 1937,
	'first_offset' : 1939,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'naval_doctrine_folder'
},

'carrier_task_force' : {
	'carrier_protection' : {
		'efficiency'  : 0.05
	},
	'allow' : {
		'carrier_group_doctrine' : 3,
		'light_cruiser_escort_role' : 3
	},
	'research_bonus_from' : {
		'naval_doctrine_practical' : 0.7,
		'base_strike_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'base_strike_doctrine',
	'difficulty' : 5,
	'start_year' : 1941,
	'folder' : 'naval_doctrine_folder'
},

'naval_underway_repleshment' : {
	'sortie' : {
		'reduction_modifier' : -0.05
	},
	'allow' : {
		'fleet_auxiliary_carrier_doctrine' : 1
	},
	'research_bonus_from' : {
		'cruiser_practical' : 0.3,
		'naval_doctrine_practical' : 0.4,
		'base_strike_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'base_strike_doctrine' ,
	'difficulty' : 5,
	'start_year' : 1937,
	'first_offset' : 1939,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'naval_doctrine_folder'
},

'radar_training' : {
	'carrier' : {	'surface_detection' : 1 },
	'escort_carrier' : { 'surface_detection' : 1 },
	'allow' : {
		'fleet_auxiliary_carrier_doctrine' : 1,
		'radar' : 1
	},
	'research_bonus_from' : {
		'cruiser_practical' : 0.3,
		'naval_doctrine_practical' : 0.4,
		'base_strike_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'base_strike_doctrine' ,
	'difficulty' : 5,
	'start_year' : 1939,
	'first_offset' : 1940,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'naval_doctrine_folder'
},

'sea_lane_defence' : {
	'allow_escorts' : 'yes',
	'research_bonus_from' : {
		'naval_doctrine_practical' : 0.7,
		'fleet_in_being_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'fleet_in_being_doctrine',
	'difficulty' : 5,
	'start_year' : 1936,
	'folder' : 'naval_doctrine_folder'
},

'destroyer_escort_role' : {
	'destroyer' : {
		'positioning' : 0.05
	},
	'escort_efficiency' : 0.05,
	'allow' : {
		'sea_lane_defence' : 1
	},
	'research_bonus_from' : {
		'destroyer_practical' : 0.3,
		'naval_doctrine_practical' : 0.4,
		'fleet_in_being_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'fleet_in_being_doctrine',
	'difficulty' : 5,
	'start_year' : 1937,
	'first_offset' : 1939,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'naval_doctrine_folder'
},


'battlefleet_concentration_doctrine' : {
	'battleship' : {
		'positioning' : 0.05
	},
	'super_heavy_battleship' : {
		'positioning' : 0.05
	},
	'allow' : {
		'sea_lane_defence' : 1
	},
	'research_bonus_from' : {
		'capitalship_practical' : 0.3,
		'naval_doctrine_practical' : 0.4,
		'fleet_in_being_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'fleet_in_being_doctrine',
	'difficulty' : 5,
	'start_year' : 1937,
	'first_offset' : 1939,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'naval_doctrine_folder'
},

'destroyer_crew_training' : {
	'destroyer' : {
		'default_organisation' : 5 ,
		'default_morale' : 0.05
	},
	'allow' : {
		'sea_lane_defence' : 1
	},
	'research_bonus_from' : {
		'destroyer_practical' : 0.3,
		'naval_doctrine_practical' : 0.4,
		'fleet_in_being_doctrine' : 0.3
	},
	'change' : 'no', 
	'on_completion' : 'fleet_in_being_doctrine',
	'difficulty' : 5,
	'start_year' : 1937,
	'first_offset' : 1939,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'naval_doctrine_folder'
},

'battleship_crew_training' : {
	'battleship' : {
		'default_organisation' : 5 ,
		'default_morale' : 0.05
	},
	'super_heavy_battleship' : {
		'default_organisation' : 5 ,
		'default_morale' : 0.05
	},
	'allow' : {
		'sea_lane_defence' : 1
	},
	'research_bonus_from' : {
		'capitalship_practical' : 0.3,
		'naval_doctrine_practical' : 0.4,
		'fleet_in_being_doctrine' : 0.3
	},
	'change' : 'no', 
	'on_completion' : 'fleet_in_being_doctrine',
	'difficulty' : 5,
	'start_year' : 1937,
	'first_offset' : 1939,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'naval_doctrine_folder'
},


'commerce_defence' : {
	'convoy_escort' : {
		'efficiency' : 0.05
	},
	'allow' : {
		'destroyer_escort_role' : 3,
		'battlefleet_concentration_doctrine' : 3
	},
	'research_bonus_from' : {
		'naval_doctrine_practical' : 0.7,
		'fleet_in_being_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'fleet_in_being_doctrine',
	'difficulty' : 5,
	'start_year' : 1941,
	'folder' : 'naval_doctrine_folder'
},

'fire_control_system_training' : {
	'targeting_chance' : 0.05,
	'allow' : {
		'sea_lane_defence' : 1,
		'mechnical_computing_machine' : 1
	},
	'research_bonus_from' : {
		'naval_doctrine_practical' : 0.7,
		'fleet_in_being_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'fleet_in_being_doctrine',
	'difficulty' : 5,
	'start_year' : 1937,
	'first_offset' : 1939,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'naval_doctrine_folder'
}	,

'commander_decision_making' : {
	'targeting_choice' : 0.05,
	'allow' : {
		'sea_lane_defence' : 1
	},
	'research_bonus_from' : {
		'naval_doctrine_practical' : 0.7,
		'fleet_in_being_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'fleet_in_being_doctrine',
	'difficulty' : 5,
	'start_year' : 1937,
	'first_offset' : 1939,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'naval_doctrine_folder'
},	

'fleet_auxiliary_submarine_doctrine' : {
	'submarine' : {
		'positioning' : 0.03
	},
	'research_bonus_from' : {
		'naval_doctrine_practical' : 0.7,
		'sealane_interdiction_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'sealane_interdiction_doctrine',
	'difficulty' : 5,
	'start_year' : 1936,
	'folder' : 'naval_doctrine_folder'
},

'trade_interdiction_submarine_doctrine' : {
	'convoy_raid' : {
		'efficiency' : 0.06
 	},
	'allow' : {
		'fleet_auxiliary_submarine_doctrine' : 1
	},
	'research_bonus_from' : {
		'submarine_practical' : 0.3,
		'naval_doctrine_practical' : 0.4,
		'sealane_interdiction_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'sealane_interdiction_doctrine',
	'difficulty' : 5,
	'start_year' : 1937,
	'first_offset' : 1939,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'naval_doctrine_folder'
},

'cruiser_warfare' : {
	'heavy_cruiser' : {
		'positioning' : 0.05
	},
	'battlecruiser' : {
		'positioning' : 0.05
	},
	'allow' : {
		'fleet_auxiliary_submarine_doctrine' : 1
	},
	'research_bonus_from' : {
		'cruiser_practical' : 0.15,
		'capitalship_practical' : 0.15,
		'naval_doctrine_practical' : 0.4,
		'sealane_interdiction_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'sealane_interdiction_doctrine',
	'difficulty' : 5,
	'start_year' : 1937,
	'first_offset' : 1939,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'naval_doctrine_folder'
},

'submarine_crew_training' : {
	'submarine' : {
		'default_organisation' : 6 ,
		'default_morale' : 0.06
	},
	'allow' : {
		'fleet_auxiliary_submarine_doctrine' : 1
	},
	'research_bonus_from' : {
		'submarine_practical' : 0.3,
		'naval_doctrine_practical' : 0.4,
		'sealane_interdiction_doctrine' : 0.3
	},
	'change' : 'no', 
	'on_completion' : 'sealane_interdiction_doctrine',
	'difficulty' : 5,
	'start_year' : 1937,
	'first_offset' : 1939,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'naval_doctrine_folder'
},

'cruiser_crew_training' : {
	'heavy_cruiser' : {
		'default_organisation' : 5 ,
		'default_morale' : 0.05
	},
	'battlecruiser' : {
		'default_organisation' : 5 ,
		'default_morale' : 0.05
	},
	'allow' : {
		'fleet_auxiliary_submarine_doctrine' : 1
	},
	'research_bonus_from' : {
		'cruiser_practical' : 0.15,
		'capitalship_practical' : 0.15,
		'naval_doctrine_practical' : 0.4,
		'sealane_interdiction_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'sealane_interdiction_doctrine',
	'difficulty' : 5,
	'start_year' : 1937,
	'first_offset' : 1939,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'naval_doctrine_folder'
},

'unrestricted_submarine_warfare_doctrine' : {
	'convoy_raid' : {
		'reduction_modifier' : -0.50 
	},
	'allow' : {
		'trade_interdiction_submarine_doctrine' : 3,
		'submarine_crew_training' : 3
	},
	'research_bonus_from' : {
		'naval_doctrine_practical' : 0.7,
		'sealane_interdiction_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'sealane_interdiction_doctrine',
	'difficulty' : 5,
	'start_year' : 1941,
	'folder' : 'naval_doctrine_folder'
},

'spotting' : {
	'destroyer' : {
		'surface_detection' : 1
	},
	'light_cruiser' : {
		'surface_detection' : 1
	},
	'allow' : {
		'fleet_auxiliary_submarine_doctrine' : 1
	},
	'research_bonus_from' : {
		'naval_doctrine_practical' : 0.7,
		'sealane_interdiction_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'sealane_interdiction_doctrine',
	'difficulty' : 5,
	'start_year' : 1937,
	'first_offset' : 1939,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'naval_doctrine_folder'
},

'basing' : {
	'naval_base_efficiency' : 0.20, 
	'allow' : {
		'fleet_auxiliary_submarine_doctrine' : 1
	},
	'research_bonus_from' : {
		'naval_doctrine_practical' : 0.7,
		'sealane_interdiction_doctrine' : 0.3
	},
	'change' : 'no' ,
	'on_completion' : 'sealane_interdiction_doctrine',
	'difficulty' : 7,
	'start_year' : 1937,
	'first_offset' : 1939,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'naval_doctrine_folder'
}
}
naval_tech = {

'destroyer_technology' : {
	'activate_building' : 'naval_base'	,
	'activate_unit' : 'destroyer',
	'research_bonus_from' : {
		'naval_engineering' : 1.0
	},
	'on_completion' :'naval_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'folder' : 'smallship_folder'
},

'destroyer_armament' : {
	'destroyer' : {
		'sea_attack' : 0.25,
		'convoy_attack' : 0.5
	},
	'allow' : {
		'destroyer_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'destroyer_practical' : 0.6,
		'fleet_in_being_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 year
	'max_level' : 12,
	'folder' : 'smallship_folder',
	'can_upgrade' : 'no'
},

'destroyer_antiaircraft' : {
	'destroyer' : {
		'air_attack' : 1.25,
		'air_defence' : 1.00
	},
	'allow' : {
		'destroyer_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'destroyer_practical' : 0.6,
		'fleet_in_being_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 year
	'max_level' : 12,
	'folder' : 'smallship_folder'
},

'destroyer_engine' : {
	'destroyer' : {
		'fuel_consumption' : 0.01,
		'range' : 400,
		'maximum_speed' : 2,
		'sea_defence' : 2.0
	},
	'allow' : {
		'destroyer_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'destroyer_practical' : 0.6,
		'fleet_in_being_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 year
	'max_level' : 12,
	'folder' : 'smallship_folder',
	'can_upgrade' : 'no'
},

'destroyer_armour' : {
	'destroyer' : {
		'hull' : 0.05
	},
	'allow' : {
		'destroyer_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'destroyer_practical' : 0.6,
		'fleet_in_being_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 year
	'max_level' : 12,
	'folder' : 'smallship_folder',
	'can_upgrade' : 'no'
},

'lightcruiser_technology' : {
	'activate_building' : 'coastal_fort',
	'activate_unit' : 'light_cruiser',
	'allow' : {
		'num_of_ports'  :1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'destroyer_practical' : 0.7
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'folder' : 'smallship_folder'
},

'lightcruiser_armament' : {
	'light_cruiser' : {
		'sea_attack' : 0.5,
		'convoy_attack' : 0.5
	},
	'allow' : {
		'lightcruiser_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'cruiser_practical' : 0.6,
		'base_strike_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 year
	'max_level' : 12,
	'folder' : 'smallship_folder',
	'can_upgrade' : 'no'
},

'lightcruiser_antiaircraft' : {
	'light_cruiser' : {
		'air_attack' : 1.50,
		'air_defence' : 1.50
	},
	'allow' : {
		'lightcruiser_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'cruiser_practical' : 0.6,
		'base_strike_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 year
	'max_level' : 12,
	'folder' : 'smallship_folder'
},

'lightcruiser_engine' : {
	'light_cruiser' : {
		'range' : 500,
		'maximum_speed' : 1.5,
		'fuel_consumption' : 0.01,
		'sea_defence' : 1.5
	},
	'allow' : {
		'lightcruiser_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'cruiser_practical' : 0.6,
		'base_strike_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 year
	'max_level' : 12,
	'folder' : 'smallship_folder',
	'can_upgrade' : 'no'
},

'lightcruiser_armour' : {
	'light_cruiser' : {
		'hull' : 0.05
	},
	'allow' : {
		'lightcruiser_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'cruiser_practical' : 0.6,
		'base_strike_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 year
	'max_level' : 12,
	'folder' : 'smallship_folder',
	'can_upgrade' : 'no'
},

'smallwarship_radar' : {
	'light_cruiser' : {
		'surface_detection' : 1.00,
		'air_detection' : 1.00,
		'night' : { 
			'attack' : 0.1
		}
	},
	'destroyer' : {
		'surface_detection' : 1.00,
		'air_detection' : 1.00,
		'night' : { 
			'attack' : 0.1
		}
	},
	'allow' : {
		'OR' : {
			'lightcruiser_technology' : 1,
			'destroyer_technology' : 1
		},
		'OR' : {	
			'AND' : {
				'NOT' : { 'smallwarship_radar' : 3 },	
				'radar' : 1
			},
			'AND' : {
				'smallwarship_radar' : 3,
				'radar' : 3
			}
		}
	},
	'research_bonus_from' : {
		'cruiser_practical' : 0.25,
		'destroyer_practical' : 0.25,
		'electornicegineering_theory'  : 0.50
	},
	'on_completion' : 'electornicegineering_theory',
	'difficulty' : 2,
	'start_year' : 1939,
	'first_offset' : 1940,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'smallship_folder'
},

'smallwarship_asw' : {
	'light_cruiser' : {
		'sub_detection' : 3,
		'sub_attack' : 1.00
	},
	'destroyer' : {
		'sub_detection' : 4,
		'sub_attack' : 2.00
	},
	'allow' : {
		'OR' : {
			'lightcruiser_technology' : 1,
			'destroyer_technology' : 1
		},
		'OR' : {
			'AND' : {
				'NOT' : { 'smallwarship_asw' : 3 },
				'radar' : 1
			},
			'AND' : {
				'smallwarship_asw' : 3,
				'radar' : 3
			}
		}
	},
	'research_bonus_from' : {
		'cruiser_practical' : 0.25,
		'destroyer_practical' : 0.25,
		'electornicegineering_theory'  : 0.50
	},
	'on_completion' : 'electornicegineering_theory',
	'difficulty' : 3,
	'start_year' : 1938,
	'first_offset' : 1940,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'smallship_folder'
},

'heavycruiser_technology' : {
	'activate_unit' : 'heavy_cruiser' ,
	'allow' : {
		'num_of_ports'  :1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'cruiser_practical' : 0.7
	},
		'on_completion' : 'naval_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'folder' : 'capitalship_folder'
},

'heavycruiser_armament' : {
	'heavy_cruiser' : {
		'sea_attack' : 1,
		'convoy_attack' : 0.5
	},
	'allow' : {
		'heavycruiser_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'cruiser_practical' : 0.6,
		'sealane_interdiction_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 year
	'max_level' : 12,
	'folder' : 'capitalship_folder',
	'can_upgrade' : 'no'
},

'heavycruiser_antiaircraft' : {
	'heavy_cruiser' : {
		'air_attack' : 1.50,
		'air_defence' : 1.50
	},
	'allow' : {
		'heavycruiser_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'cruiser_practical' : 0.6,
		'sealane_interdiction_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 year
	'max_level' : 12,
	'folder' : 'capitalship_folder'
},

'heavycruiser_engine' : {
	'heavy_cruiser' : {
		'fuel_consumption' : 0.01,
		'range' : 600,
		'maximum_speed' : 1.5,
		'sea_defence' : 1.0		
	},
	'allow' : {
		'heavycruiser_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'cruiser_practical' : 0.6,
		'sealane_interdiction_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 year
	'max_level' : 12,
	'folder' : 'capitalship_folder',
	'can_upgrade' : 'no'
},

'heavycruiser_armour' : {
	'heavy_cruiser' : {
		'hull' : 0.06
	},
	'allow' : {
		'heavycruiser_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'cruiser_practical' : 0.6,
		'sealane_interdiction_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 year
	'max_level' : 12,
	'folder' : 'capitalship_folder',
	'can_upgrade' : 'no'
},

'battlecruiser_technology' : {
	'activate_unit' : 'battlecruiser' ,
	'allow' : {
		'num_of_ports'  :1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'capitalship_practical' : 0.7
	},
		'on_completion' : 'naval_engineering',
	'difficulty' : 2,
	'start_year' : 1918,
	'folder' : 'capitalship_folder'
},

'battleship_technology' : {
	'activate_unit' : 'battleship' ,
	'allow' : {
		'num_of_ports'  :1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'capitalship_practical' : 0.7
	},
		'on_completion' : 'naval_engineering',
	'difficulty' : 3,
	'start_year' : 1918,
	'folder' : 'capitalship_folder'
},

'capitalship_armament' : {
	'battlecruiser' : {
		'sea_attack' : 1.5,
		'convoy_attack' : 1
	},
	'battleship' : {
		'sea_attack' : 2,
		'convoy_attack' : 1
	},
	'allow' : {
		'OR' : {
			'battlecruiser_technology' : 1,
			'battleship_technology' : 1
		}
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'capitalship_practical' : 0.7
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 3,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 year
	'max_level' : 12,
	'folder' : 'capitalship_folder',
	'can_upgrade' : 'no'
},

'battlecruiser_antiaircraft' : {
	'battlecruiser' : {
		'air_attack' : 0.50,
		'air_defence' : 1.00
	},
	'allow' : {
		'battlecruiser_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'capitalship_practical' : 0.6,
		'sealane_interdiction_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 2,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 year
	'max_level' : 12,
	'folder' : 'capitalship_folder'
},

'battlecruiser_engine' : {
	'battlecruiser' : {
		'fuel_consumption' : 0.01,
		'range' : 600,
		'maximum_speed' : 1,
		'sea_defence' : 0.5	
	},
	'allow' : {
		'battlecruiser_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'capitalship_practical' : 0.6,
		'sealane_interdiction_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 2,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 year
	'max_level' : 12,
	'folder' : 'capitalship_folder',
	'can_upgrade' : 'no'
},

'battlecruiser_armour' : {
	'battlecruiser' : {
		'hull' : 0.08
	},
	'allow' : {
		'battlecruiser_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'capitalship_practical' : 0.6,
		'sealane_interdiction_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 2,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 year
	'max_level' : 12,
	'folder' : 'capitalship_folder',
	'can_upgrade' : 'no'
},

'battleship_antiaircraft' : {
	'battleship' : {
		'air_attack' : 0.5,
		'air_defence' : 1.00
	},
	'super_heavy_battleship' : {
		'air_attack' : 0.35,
		'air_defence' : 0.65
	}	,
	'allow' : {
		'battleship_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'capitalship_practical' : 0.6,
		'fleet_in_being_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 3,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 year
	'max_level' : 12,
	'folder' : 'capitalship_folder'
},

'battleship_engine' : {
	'battleship' : {
		'fuel_consumption' : 0.01,
		'range' : 600,
		'maximum_speed' : 0.5,
		'sea_defence' : 0.25		
	},
	'allow' : {
		'battleship_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'capitalship_practical' : 0.6,
		'fleet_in_being_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 3,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 year
	'max_level' : 12,
	'folder' : 'capitalship_folder',
	'can_upgrade' : 'no'
},

'battleship_armour' : {
	'battleship' : {
		'hull' : 0.10
	},
	'allow' : {
		'battleship_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'capitalship_practical' : 0.6,
		'fleet_in_being_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 3,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 year
	'max_level' : 12,
	'folder' : 'capitalship_folder',
	'can_upgrade' : 'no'
},

'super_heavy_battleship_technology' : {
	'activate_unit' : 'super_heavy_battleship' ,
	'allow' : {
		'battleship_technology'  : 1,
		'capitalship_armament' : 2,
		'battleship_antiaircraft' : 2,
		'battleship_engine' : 2,
		'battleship_armour' : 2
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'capitalship_practical' : 0.7
	},
		'on_completion' : 'naval_engineering',
	'difficulty' : 4,
	'start_year' : 1938,
	'folder' : 'capitalship_folder'
},

'cag_development' : {
	'activate_unit' : 'cag',
	'allow' : { 
		'num_of_ports' : 1,
		'single_engine_aircraft_design' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'single_engine_aircraft_practical' : 0.3,
		'capitalship_practical' : 0.4
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'folder' : 'capitalship_folder'
}	,

'escort_carrier_technology' : {
	'activate_unit' : 'escort_carrier',
	'allow' : {
		'num_of_ports'  : 1,
		'cag_development' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'capitalship_practical' : 0.6,
		'base_strike_doctrine' : 0.1	
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 2,
	'start_year' : 1918,
	'folder' : 'capitalship_folder'
},

'carrier_technology' : {
	'activate_unit' : 'carrier',
	'allow' : {
		'num_of_ports'  : 1,
		'escort_carrier_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'capitalship_practical' : 0.6,
		'base_strike_doctrine' : 0.1	
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 3,
	'start_year' : 1918,
	'folder' : 'capitalship_folder'
},

'carrier_antiaircraft' : {
	'carrier' : {
		'air_attack' : 0.50,
		'air_defence' : 1.00
	},
	'escort_carrier' : {
		'air_attack' : 0.50,
		'air_defence' : 1.00
	},
	'allow' : {
		'carrier_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'carrier_practical' : 0.6,
		'base_strike_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 3,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 year
	'max_level' : 12,
	'folder' : 'capitalship_folder'
},

'carrier_engine' : {
	'carrier' : {
		'fuel_consumption' : 0.01,
		'range' : 600,
		'maximum_speed' : 1,
		'sea_defence' : 0.5
	},
	'escort_carrier' : {
		'fuel_consumption' : 0.01,
		'range' : 500,
		'maximum_speed' : 0.5,
		'sea_defence' : 1
	},
	'allow' : {
		'carrier_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'carrier_practical' : 0.6,
		'base_strike_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 3,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 year
	'max_level' : 12,
	'folder' : 'capitalship_folder',
	'can_upgrade' : 'no'
},

'carrier_armour' : {
	'carrier' : {
		'hull' : 0.10
	},
	'escort_carrier' : {
		'hull' : 0.08
	},
	'allow' : {
		'carrier_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'carrier_practical' : 0.6,
		'base_strike_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 3,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2	,#one new every 2 year
	'max_level' : 12,
	'folder' : 'capitalship_folder',
	'can_upgrade' : 'no'
},

'carrier_hanger' : {
	'carrier' : {
		'visibility' : -3
	},
	'escort_carrier' : {
		'visibility' : -3
	},
	'allow' : {
		'carrier_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 0.3,
		'carrier_practical' : 0.6,
		'base_strike_doctrine' : 0.1
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 3,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 2,	#one new every 2 year
	'max_level' : 12,
	'folder' : 'capitalship_folder',
	'can_upgrade' : 'no'
},

'largewarship_radar' : {
	'heavy_cruiser' : {
		'surface_detection' : 1.00,
		'air_detection' : 1.00,
		'night' : { 
			'attack' : 0.1
		}
	},
	'battlecruiser' : {
		'surface_detection' : 1.00,
		'air_detection' : 1.00,
		'night' : { 
			'attack' : 0.1
		}
	},
	'battleship' : {
		'surface_detection' : 1.00,
		'air_detection' : 1.00,
		'night' : { 
			'attack' : 0.1
		}
	},
	'super_heavy_battleship' : {
		'surface_detection' : 1.00,
		'air_detection' : 1.00,
		'night' : { 
			'attack' : 0.1
		}
	},
	'carrier' : {
		'surface_detection' : 1.00,
		'air_detection' : 1.00,
		'night' : { 
			'attack' : 0.1
		}
	},
	'escort_carrier' : {
		'surface_detection' : 1.00,
		'air_detection' : 1.00,
		'night' : { 
			'attack' : 0.1
		}
	},
	'allow' : {
		'OR' : {
			'heavycruiser_technology' : 1,
			'battlecruiser_technology' : 1,
			'battleship_technology' : 1,
			'carrier_technology' : 1
		},
		'OR' : {	
			'AND' : {
				'NOT' : { 'largewarship_radar' : 3 },	
				'radar' : 1
			},
			'AND' : {
				'largewarship_radar' : 3,
				'radar' : 3
			}
		}
	},
	'research_bonus_from' : {
		'cruiser_practical' : 0.20,
		'capitalship_practical' : 0.20,
		'carrier_practical' : 0.20,
		'electornicegineering_theory'  : 0.40
	},
	'on_completion' : 'electornicegineering_theory',
	'difficulty' : 2,
	'start_year' : 1939,
	'first_offset' : 1940,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'capitalship_folder'
},

'submarine_technology' : {
	'activate_unit' : 'submarine',
	'allow' : {
		'num_of_ports' : 1
	},
	'research_bonus_from' : {
		'submarine_engineering' : 1	
	},
	'on_completion' : 'submarine_engineering',
	'difficulty' : 7,
	'start_year' : 1918,
	'folder' : 'smallship_folder'
},

'submarine_antiaircraft' : {
	'submarine' : {
		'air_attack' :  0.25,
		'air_defence' : 1.00
	},
	'allow' : {
		'submarine_technology' : 1
	},
	'research_bonus_from' : {
		'submarine_engineering' : 0.3,
		'submarine_practical' : 0.6,
		'sealane_interdiction_doctrine' : 0.1
	},
	'on_completion' : 'submarine_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 3	,#one new every 2 year
	'max_level' : 12,
	'folder' : 'smallship_folder',
	'can_upgrade' : 'yes'
},

'submarine_engine' : {
	'submarine' : {
		'fuel_consumption' : 0.015,
		'range' : 900,
		'maximum_speed' : 1.5,
		'sea_defence' : 0.25
	},
	'allow' : {
		'submarine_technology' : 1
	},
	'research_bonus_from' : {
		'submarine_engineering' : 0.3,
		'submarine_practical' : 0.6,
		'sealane_interdiction_doctrine' : 0.1
	},
	'on_completion' : 'submarine_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 3,	#one new every 2 year
	'max_level' : 12,
	'folder' : 'smallship_folder',
	'can_upgrade' : 'no'
},

'submarine_hull' : {
	'submarine' : {
		'visibility' : -1
	},
	'allow' : {
		'submarine_technology' : 1
	},
	'research_bonus_from' : {
		'submarine_engineering' : 0.3,
		'submarine_practical' : 0.6,
		'sealane_interdiction_doctrine' : 0.1
	},
	'on_completion' : 'submarine_engineering',
	'difficulty' : 3,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 3,	#one new every 2 year
	'max_level' : 12,
	'folder' : 'smallship_folder',
	'can_upgrade' : 'no'
},

'submarine_torpedoes' : {
	'submarine' : {
		'sea_attack' : 0.25,
		'convoy_attack' : 4.5
	},
	'allow' : {
		'submarine_technology' : 1
	},
	'research_bonus_from' : {
		'submarine_engineering' : 0.3,
		'submarine_practical' : 0.6,
		'sealane_interdiction_doctrine' : 0.1
	},
	'on_completion' : 'submarine_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1934,	#2nd model is from 1936
	'additional_offset' : 3,	#one new every 2 year
	'max_level' : 12,
	'folder' : 'smallship_folder',
	'can_upgrade' : 'yes'
},


'submarine_sonar' : {
	'submarine' : {
		'surface_detection' : 2,
		'sub_detection' : 1,
		'sea_defence' : 0.75 
	},
	'allow' : {
		'submarine_technology' : 1,
		'radar' : 1
	},
	'research_bonus_from' : {
		'electornicegineering_theory' : 0.3,
		'submarine_practical' : 0.7
	},
	'on_completion' : 'electornicegineering_theory'  ,
	'difficulty' : 3,
	'start_year' : 1939,
	'first_offset' : 1940,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'smallship_folder'
},

'submarine_airwarningequipment' : {
	'submarine' : {
		'air_detection' : 0.5
	},
	'allow' : {
		'submarine_technology' : 1,
		'radar' : 1
	},
	'research_bonus_from' : {
		'electornicegineering_theory'  : 0.3,
		'submarine_practical' : 0.7
	},
	'on_completion' : 'electornicegineering_theory' , 
	'difficulty' : 1,
	'start_year' : 1939,
	'first_offset' : 1940,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'smallship_folder'
},

'amphibious_invasion_craft' : {
	'activate_unit' : 'invasion_transport_ship',
	'allow' : {
		'destroyer_technology' : 1
	},
	'research_bonus_from' : {
		'naval_engineering' : 1.0
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 8,
	'start_year' : 1940,
	'folder' : 'smallship_folder'
},

'advanced_invasion_craft' : {
	'activate_unit' : 'assault_ship',
	'research_bonus_from' : {
		'naval_engineering' : 1.0
	},
	'allow' : {
		'amphibious_invasion_craft' : 1,
		'amphibious_invasion_technology' : 2,
		'amphibious_assault_units' : 2
	},
	'on_completion' : 'naval_engineering',
	'difficulty' : 10,
	'start_year' : 1942,
	'folder' : 'smallship_folder'
},

'amphibious_invasion_technology' : {
	'transport_ship' : {
		'amphibious_invasion_speed' : 0.10
	},
	'invasion_transport_ship' : {
		'amphibious_invasion_speed' : 0.10
	},
	'assault_ship' : {
		'amphibious_invasion_speed' : 0.10
	},
	'research_bonus_from' : {
		'naval_engineering' : 1.0
	},
	'allow' : {
		'amphibious_invasion_craft' : 1
	},
	'change' : 'no',
	'on_completion' : 'naval_engineering',
	'difficulty' : 4,
	'start_year' : 1940,
	'first_offset' : 1942,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'smallship_folder'
},

'amphibious_assault_units' : {
	'transport_ship' : {
		'amphibious_invasion_defence' : 0.10
	},
	'invasion_transport_ship' : {
		'amphibious_invasion_defence' : 0.10
	},
	'assault_ship' : {
		'amphibious_invasion_defence' : 0.10
	},
	'research_bonus_from' : {
		'naval_engineering' : 1.0
	},
	'allow' : {
		'amphibious_invasion_craft' : 1
	},
	'change' : 'no',
	'on_completion' : 'naval_engineering',
	'difficulty' : 5,
	'start_year' : 1940,
	'first_offset' : 1942,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'smallship_folder'
}
}
stratgic_resources = {

'aluminium' : {
	'air_build_speed' : -0.15
},
'rubber' : {
	'amm_movement_speed' : 0.15
},
'heavy_water' : {
	'nuke_research' : 0.4
},
'uranium' : {
	'nuke_research' : 0.4
},
'tungsten' : {
	'hard_attack' : 0.15
},
'fur' : {
	'winter_effects' : 0.15
},
'black_soil' : {
	'global_manpower_modifier' : 0.10
},
'cinchona' : {
	'jungle_effects' : 0.15
},
'helium' : {
	'rocket_build_speed' : -0.15
},
'gold' : {
	'global_money' : 0.33
},
'horses' : {
	'supply_throughput' : 0.10
},
'antibiotics' : {
	'casualty_trickleback' : 0.05
},
'ballbearings' : {
	'unit_repair' : 0.15
},
'prefab_ship_facilities' : {
	'naval_base_efficiency' : 0.33
},
'dockyard_facilities' : {
	'naval_build_speed' : -0.15
},
'oil_refinery' : {
	'fuel_conversion' : 0.5
},
'automotive_industry' : {
	'tank_build_speed' : -0.15
}
}
secret_tech = {

'strategic_rocket_development' : {
	'allow' : {
		'rocket_engine' : 1
	},
	'research_bonus_from' : {
		'rocket_science' : 1.0
	},
	'on_completion' : 'rocket_science',
	'difficulty' : 4,
	#common for all techs.
	'start_year' : 1940,
	'folder' : 'secretweapon_folder'
}	,

'flyingbomb_development' : {
	'activate_unit' : 'flying_bomb' ,
	'allow' : {
		'strategic_rocket_development' : 1
	},
	'research_bonus_from' : {
		'rocket_science' : 1.0
	},
	'on_completion' : 'rocket_science',
	'difficulty' : 4,
	#common for all techs.
	'start_year' : 1942,
	'folder' : 'secretweapon_folder'
}	,

'flyingrocket_development' : {
	'activate_unit' : 'flying_rocket' ,
	'allow' : {
		'flyingbomb_development' : 1
	},
	'research_bonus_from' : {
		'rocket_science' : 0.7,
		'rocket_practical' : 0.3
	},
	'on_completion' : 'rocket_science',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1943,
	'folder' : 'secretweapon_folder'
},	

'strategicrocket_engine' : {
	'flying_rocket' : {
		'strategic_attack' : -5,
		'range' : 50
	},
	'allow' : {
		'flyingrocket_development' : 1
	},
	'research_bonus_from' : {
		'rocket_science' : 0.7,
		'rocket_practical' : 0.3
	},
	'on_completion' : 'rocket_science',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1944,
	'first_offset' : 1945,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'secretweapon_folder'
},	

'strategicrocket_warhead' : {
	'flying_rocket' : {
		'strategic_attack' : 10,
		'range' : -75
	},
	'allow' : {
		'flyingrocket_development' : 1
	},
	'research_bonus_from' : {
		'rocket_science' : 0.7,
		'rocket_practical' : 0.3
	},
	'on_completion' : 'rocket_science',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1944,
	'first_offset' : 1945,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'secretweapon_folder'
}	,

'strategicrocket_structure' : {
	'flying_rocket' : {
		'range' : 50
	},
	'allow' : {
		'flyingrocket_development' : 1
	},
	'research_bonus_from' : {
		'rocket_science' : 0.7,
		'rocket_practical' : 0.3
	},
	'on_completion' : 'rocket_science',
	'difficulty' : 5,
	#common for all techs.
	'start_year' : 1944,
	'first_offset' : 1945,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'secretweapon_folder'
}	,

'da_bomb' : {
	'nuclear_production' : 0.1,
	'allow' : {
		'civil_nuclear_research' : 4,
		'any_owned_province' : {
			'has_building' : 'nuclear_reactor'
		}
	},
	'research_bonus_from' : {
		'nuclear_bomb' : 0.9,
		'nuclear_physics' : 0.1
	},
	'on_completion' : 'nuclear_physics',
	'difficulty' : 10,
	'is_nuclear' : 'yes',
	#common for all techs.
	'start_year' : 1943,
	'first_offset' : 1944,
	'additional_offset' : 2,
	'max_level' : 12,
	'folder' : 'secretweapon_folder'
},

'radar_guided_missile' : {
	'cas' : {
		'soft_attack' : 3,
		'hard_attack' : 3
	},
	'tactical_bomber' : {
		'soft_attack' : 3,
		'hard_attack' : 3
	},
	'allow' : {
		'rocket_engine' : 1,
		'cas_development' : 1,
		'radar' : 3,
		'aeroengine' : 2,
		'single_engine_airframe' : 2
	},
	'research_bonus_from' : {
		'single_engine_aircraft_practical' : 0.35,
		'twin_engine_aircraft_practical' : 0.35,
		'rocket_science' : 0.3
	},
	'on_completion' : 'rocket_science',
	'difficulty' : 5,
	'start_year' : 1944,
	'folder' : 'secretweapon_folder'
}	,

'radar_guided_bomb' : {
	'tactical_bomber' : {
		'soft_attack' : 2,
		'hard_attack' : 2,
		'sea_attack' : 2
	},
	'naval_bomber' : {
		'sea_attack' : 3
	},
	'allow' : {
		'medium_bomb' : 3,
		'radar' : 3,
		'aeroengine' : 2,
		'twin_engine_airframe' : 2
	},
	'research_bonus_from' : {
		'single_engine_aircraft_practical' : 0.35,
		'twin_engine_aircraft_practical' : 0.35,
		'aeronautic_engineering' : 0.3
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 5,
	'start_year' : 1944,
	'folder' : 'secretweapon_folder'
},

'electric_powered_torpedo' : {
	'submarine' : {
		'sea_attack' : 2,
		'convoy_attack' : 4,
		'sea_defence' : 2,
		'positioning' :  0.2
	},
	'allow' : {
		'submarine_torpedoes' : 4,
		'submarine_engine' : 4
	},
	'research_bonus_from' : {
		'submarine_engineering' : 0.3,
		'submarine_practical' : 0.6,
		'sealane_interdiction_doctrine' : 0.1
	},
	'on_completion' : 'submarine_engineering',
	'difficulty' : 5,
	'start_year' : 1945,
	'folder' : 'secretweapon_folder'
},

'helecopters' : {
	'allow' : {
		'single_engine_airframe' : 4,
		'aeroengine' : 4
	},
	'research_bonus_from' : {
		'single_engine_aircraft_practical' : 0.7,
		'aeronautic_engineering' : 0.3
	}, 	
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 5,
	'start_year' : 1945,
	'folder' : 'secretweapon_folder'
},

'medical_evacuation' : {
	'infantry_brigade' : {
		'default_morale' : 0.1
	},
	'marine_brigade' : {
		'default_morale' : 0.1
	},
	'paratrooper_brigade' : {
		'default_morale' : 0.1
	},
	'bergsjaeger_brigade' : {
		'default_morale' : 0.1
	},
	'motorized_brigade' : {
		'default_morale' : 0.1	
	},
	'mechanized_brigade': {
		'default_morale' : 0.1
	},
	'alpini_brigade' : {
		'default_morale' : 0.1
	},
	'alpins_brigade' : {
		'default_morale' : 0.1
	},
	'gurkha_brigade' : {
		'default_morale' : 0.1
	},
	'waffen_brigade' : {
		'default_morale' : 0.1
	},
	'guards_brigade' : {
		'default_morale' : 0.1
	},
	'imperial_brigade' : {
		'default_morale' : 0.1
	},
	'ranger_brigade' : {
		'default_morale' : 0.1
	},
	'allow' : {
		'helecopters' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'land_doctrine_practical' : 0.7
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 5,
	'start_year' : 1946,
	'folder' : 'secretweapon_folder'
},

'pilot_rescue' : {
	'interceptor' : {
		'default_morale' : 0.1
	},
	'multi_role' : {
		'default_morale' : 0.1
	},
	'allow' : {
		'helecopters' : 1
	},
	'research_bonus_from' : {
		'aeronautic_engineering' : 0.3,
		'air_doctrine_practical' : 0.7
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 5,
	'start_year' : 1946,
	'folder' : 'secretweapon_folder'
},

'sam' : {
	'provincial_aa_efficiency' : 2,
	'allow' : {
		'strategicrocket_engine' : 1,
		'radar' : 3,
		'strategicrocket_structure' : 1
	},
	'research_bonus_from' : {
		'rocket_practical' : 0.7,
		'rocket_science' : 0.3
	},
	'on_completion' : 'rocket_science',
	'difficulty' : 5,
	'start_year' : 1944,
	'folder' : 'secretweapon_folder'
},

'aam' : {
	'interceptor' : {
		'air_attack' : 3
	},
	'multi_role' : {
		'air_attack' : 2
	},
	'allow' : {
		'strategicrocket_engine' : 1,
		'radar' : 3,
		'strategicrocket_structure' : 1
	},
	'research_bonus_from' : {
		'rocket_practical' : 0.7,
		'aeronautic_engineering' : 0.3
	},
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 5,
	'start_year' : 1944,
	'folder' : 'secretweapon_folder'
}
}
categories =  {
	'ocean' : {
		'movement_cost' : 1.0,
		'is_water' : 'yes'
	},
	
	'mountain' : {
		'movement_cost' : 2.0,
		'attack' : -0.5,
		'attrition' : 2,
		'precipitation' : 10,
		'temperature' : -8
	},

	'forest' : {
		'movement_cost' : 1.5,
		'attack' : -0.2,
		'attrition' : 1
	},

	'woods' : {
		'movement_cost' : 1.2,
		'attack' : -0.1,
		'attrition' : 1
	},

	'marsh' : {
		'movement_cost' : 2.0,
		'attack' : -0.4,
		'attrition' : 2,
		'humidity' : 0.2
	},

	'plains' : {
		'movement_cost' : 1.0
	},

	'urban' : {
		'movement_cost' : 1.2,
		'attack' : -0.4
	},

	'hills' : {
		'movement_cost' : 1.5,
		'attack' : -0.3,
		'precipitation' : 5,
		'temperature' : -4
	},

	'jungle' : {
		'movement_cost' : 1.5,
		'attack' : -0.4,
		'attrition' : 2,
		'humidity' : 0.1,
		'temperature' : 3
	}	,

	'desert' : {
		'movement_cost' : 1.0,
		'attrition' : 2,
		'humidity' : -50.0,
		'precipitation' : 50,
		'temperature' : 5
	},

	'arctic' : {
		'movement_cost' : 1.5,
		'attack' : -0.2,
		'attrition' : 2,
		'temperature' : -11
	}
}
theories = {

'naval_engineering_research' : {
	'allow' : {
		'num_of_ports'  :1
	},
	'decay' : { 'naval_engineering' : -0.1 },
	'on_completion' :'naval_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1936,
	'additional_offset' : 2,
	'max_level' : 10,
	'folder' : 'theory_folder'
},

'submarine_engineering_research' : {
	'allow' : {
		'num_of_ports'  :1
	},
	'decay' : { 'submarine_engineering' : -0.1 },
	'on_completion' : 'submarine_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1936,
	'additional_offset' : 2,
	'max_level' : 10,
	'folder' : 'theory_folder'
},

'aeronautic_engineering_research' : {
	'decay' : { 'aeronautic_engineering' : -0.1 },
	'on_completion' : 'aeronautic_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1936,
	'additional_offset' : 2,
	'max_level' : 10,
	'folder' : 'theory_folder'
},

'rocket_science_research' : {
	'decay' : { 'rocket_science' : -0.1 },
	'on_completion' : 'rocket_science',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1936,
	'additional_offset' : 2,
	'max_level' : 10,
	'folder' : 'theory_folder'
},

'chemical_engineering_research' : {
	'decay' : { 'chemical_engineering' : -0.1 },
	'on_completion' : 'chemical_engineering',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1936,
	'additional_offset' : 2,
	'max_level' : 10,
	'folder' : 'theory_folder'
},

'nuclear_physics_research' : {
	'decay' : { 'nuclear_physics' : -0.1 },
	'on_completion' : 'nuclear_physics',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1936,
	'additional_offset' : 2,
	'max_level' : 10,
	'folder' : 'theory_folder'
},

'jetengine_research' : {
	'decay' : { 'jetengine_theory' : -0.1 },
	'on_completion' : 'jetengine_theory',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1936,
	'additional_offset' : 2,
	'max_level' : 10,
	'folder' : 'theory_folder'
},

'mechanicalengineering_research' : {
	'decay' : { 'mechanicalengineering_theory' : -0.1 },
	'on_completion' : 'mechanicalengineering_theory',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1936,
	'additional_offset' : 2,
	'max_level' : 10,
	'folder' : 'theory_folder'
},

'automotive_research' : {
	'decay' : { 'automotive_theory' : -0.1 },
	'on_completion' : 'automotive_theory' ,
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1936,
	'additional_offset' : 2,
	'max_level' : 10,
	'folder' : 'theory_folder'
},

'electornicegineering_research' : {
	'decay' : { 'electornicegineering_theory' : -0.1 },
	'on_completion' : 'electornicegineering_theory' ,
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1936,
	'additional_offset' : 2,
	'max_level' : 10,
	'folder' : 'theory_folder'
},

'artillery_research' : {
	'decay' : { 'artillery_theory' : -0.1 },
	'on_completion' : 'artillery_theory',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1936,
	'additional_offset' : 2,
	'max_level' : 10,
	'folder' : 'theory_folder'
},

'mobile_research' : {
	'decay' : { 'mobile_theory' : -0.1 },
	'on_completion' : 'mobile_theory',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1936,
	'additional_offset' : 2,
	'max_level' : 10,
	'folder' : 'theory_folder'
},

'militia_research' : {
	'decay' : { 'militia_theory' : -0.1 },
	'on_completion' : 'militia_theory',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1936,
	'additional_offset' : 2,
	'max_level' : 10,
	'folder' : 'theory_folder'
},

'infantry_research' : {
	'decay' : { 'infantry_theory' : -0.1 },
	'on_completion' : 'infantry_theory',
	'difficulty' : 1,
	'start_year' : 1918,
	'first_offset' : 1936,
	'additional_offset' : 2,
	'max_level' : 10,
	'folder' : 'theory_folder'
}
}
units = {

'alpini_brigade' : {
	'usable_by' : 'ITA',
	'type' : 'land',
	'sprite' : 'Elite',
	'unit_group' : 'infantry_unit_type',
	#lim'ITA'tions,
	'minimum_of_type' : 6 ,
	'max_percentage_of_type' : 0.04,
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 45,
	'default_morale' : 0.50,
	'officers' : 130,
	#Building Costs,
	'build_cost_ic' : 2.67,
	'build_cost_manpower' : 4.00,
	'build_time' : 160,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 4.33,
	'transport_weight' : 8.00,
	'supply_consumption' : 0.67,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 4.00,
	'toughness' : 3.33,
	'softness' : 1.00,
	'air_defence' : 2.50,
	#Offensive Abilities,
	'suppression' : 0.67,
	'soft_attack' : 1.67,
	'hard_attack' : 0.33,
	'air_attack' : 0.10,
	'ap_attack' : 1,
	'mountain' : {
		'attack' : 0.60,
		'defence' : 0.25,
		'movement' : 0.10	
	},
    'hills' : {
		'attack' : 0.50,
		'defence' : 0.15,
		'movement' : 0.10
		},
	'arctic' : { 
		'attack' : 0.60,
		'defence' : 0.25,
		'movement' : 0.20	
		},
	'jungle' : 	{
        'attack' : 0.10,
		'movement' : 0.10
	},
	'forest' : 	{
        'attack' : 0.10,
		'movement' : 0.10
	},
	'woods' : 	{
        'attack' : 0.05,
		'movement' : 0.05
	}	,
	'combat_width' : 1,
	'completion_size' : 0.3,
	'on_completion' : 'infantry_practical',
	'priority' : 9
},

'alpins_brigade' : {
	'usable_by' :  'FRA' ,
	'type' : 'land',
	'sprite' : 'Elite',
	'unit_group' : 'infantry_unit_type',
	#lim'ITA'tions,
	'minimum_of_type' : 6 ,
	'max_percentage_of_type' : 0.04,
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 50,
	'default_morale' : 0.45,
	'officers' : 130,
	#Building Costs,
	'build_cost_ic' : 2.67,
	'build_cost_manpower' : 4.00,
	'build_time' : 160,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 4.33,
	'transport_weight' : 8.00,
	'supply_consumption' : 0.67,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 4.00,
	'toughness' : 3.33,
	'softness' : 1.00,
	'air_defence' : 2.50,
	#Offensive Abilities,
	'suppression' : 0.67,
	'soft_attack' : 1.67,
	'hard_attack' : 0.33,
	'air_attack' : 0.10,
	'ap_attack' : 1,
	'mountain' : {
		'attack' : 0.55,
		'defence' : 0.30,
		'movement' : 0.10	
	},
    'hills' : {
		'attack' : 0.45,
		'defence' : 0.20,
		'movement' : 0.10
		},
	'arctic' : { 
		'attack' : 0.55,
		'defence' : 0.30,
		'movement' : 0.20	
		},
	'jungle' : 	{
        'attack' : 0.10,
		'movement' : 0.10
	},
	'forest' : 	{
        'attack' : 0.10,
		'movement' : 0.10
	},
	'woods' : 	{
        'attack' : 0.05,
		'movement' : 0.05
	}	,
	'combat_width' : 1,
	'completion_size' : 0.3,
	'on_completion' : 'infantry_practical',
	'priority' : 9
},

'anti_air_brigade' : {
	'type' : 'land',
	'sprite' : 'Infantry',
	'unit_group' : 'direct_fire_unit_type',
	#Size Definitions,
	'max_strength' : 10,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 2.33,
	'build_cost_manpower' : 1.67,
	'build_time' : 95,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 4.00,
	'transport_weight' : 5.00,
	'supply_consumption' : 0.67,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 2.00,	# was 3.00,
	'toughness' : 1.00,
	'softness' : 0.80, #1.00,
	'air_defence' : 5.00,
	#Offensive Abilities,
	'suppression' : 0.00,
	'soft_attack' : 0.70,
	'hard_attack' : 1.00,
	'air_attack' : 5.00,
   	'amphibious' : { 'attack' : -0.40 },
   	'river' : { 'attack' : -0.05 },
	'urban' : {
		'attack' : -0.20
	},
   	'marsh' : {
        'attack' : -0.20,
		'movement' : -0.70
    },
	'jungle' : 	{
        'attack' : -0.30,
		'movement' : -0.30
    },
	'forest' : 	{
        'attack' : -0.20,
		'movement' : -0.20
    },
	'woods' : 	{
        'attack' : -0.10 ,
		'movement' : -0.10
    },
	'mountain' : 	{
        'attack' : -0.20,
		'movement' : -0.70
    },
	'hills' : 	{
		'attack' : -0.10,
        'movement' : -0.10
    },
	'combat_width' : 0,
	'completion_size' : 0.2,
	'on_completion' : 'artillery_practical',
	'priority' : 3
},

'anti_tank_brigade' : {
	'type' : 'land',
	'sprite' : 'Infantry',
	'unit_group' : 'direct_fire_unit_type',
	#Size Definitions,
	'max_strength' : 10,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 3.00,
	'build_cost_manpower' : 1.33,
	'build_time' : 100,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 4.00,
	'transport_weight' : 5.00,
	'supply_consumption' : 1.00,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 2.00,	# was 3.00,
	'toughness' : 0.67,
	'softness' : 0.80, #1.00,
	'air_defence' : 1.50,
	#Offensive Abilities,
	'suppression' : 0.00,
	'soft_attack' : 1.20,
	'hard_attack' : 2.50,
	'air_attack' : 0.10,
	'ap_attack' : 5,
   	'amphibious' : { 'attack' : -0.40 },
   	'river' : { 'attack' : -0.05 },
	'urban' : {
		'attack' : -0.20,
	},
   	'marsh' : {
        'attack' : -0.20,
		'movement' : -0.70
    },
	'jungle' : 	{
        'attack' : -0.30,
		'movement' : -0.30
    },
	'forest' : 	{
        'attack' : -0.20,
		'movement' : -0.20
    },
	'woods' : 	{
        'attack' : -0.10 ,
		'movement' : -0.10
    },
	'mountain' : 	{
        'attack' : -0.20,
		'movement' : -0.70
    },
	'hills' : 	{
		'attack' : -0.10,
        'movement' : -0.10
    },
	'combat_width' : 0,
	'completion_size' : 0.3,
	'on_completion' : 'artillery_practical',
	'priority' : 3
},

'armor_brigade' : {
	'type' : 'land',
	'sprite' : 'Tank',
	'active' : 'no',
	'unit_group' : 'armor_unit_type',
	'is_mobile' : 'yes',
	'is_armor' : 'yes',
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 12.00,
	'build_cost_manpower' : 2.33,
	'build_time' : 190,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 6.00,
	'transport_weight' : 20.00,
	'supply_consumption' : 1.66,
	'fuel_consumption' : 3.4,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 3.50,	# was 4.67,
	'toughness' : 4.00,
	'softness' : 0.15, #was 20,
	'air_defence' : 0.80,
	'armor_value' : 5,
	#Offensive Abilities,
	'suppression' : 0.00,
	'soft_attack' : 2.33,
	'hard_attack' : 2.33,
	'air_attack' : 0.33,
	'ap_attack' : 5,
    'amphibious' : { 'attack' : -0.60 },
    'river' : { 'attack' : -0.05 },
    'urban' : {
        'attack' : -0.40,
        'defence' : -0.70
    },
    'marsh' : {
        'attack' : -0.40 ,
        'movement' : -0.80,
        'defence' : -0.60
    },
    'jungle' : {
        'attack' : -0.40,
        'movement' : -0.40,
        'defence' : -0.60
    },
    'forest' : {
        'attack' : -0.30,
        'movement' : -0.40,
        'defence' : -0.40
    },
    'woods' : {
        'attack' : -0.10 ,
         'movement' : -0.10,
        'defence' : -0.30
    },
    'mountain' : {
        'attack' : -0.30 ,
        'movement' : -0.40,
        'defence' : -0.70
    },
    'hills' : {
        'attack' : -0.10   ,
        'movement' : -0.10,
        'defence' : -0.30
    },
	'combat_width' : 2,
	'completion_size' : 2,
	'on_completion' : 'armour_practical',
	'priority' : 10
},

'armored_car_brigade' : {
	'type' : 'land',
	'sprite' : 'Infantry',
	'unit_group' : 'support_unit_type',
	'is_mobile' : 'yes',
	#Size Definitions,
	'max_strength' : 10,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 4.67,
	'build_cost_manpower' : 1.67,
	'build_time' : 110,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 9.00,
	'transport_weight' : 5.00,
	'supply_consumption' : 0.5,
	'fuel_consumption' : 0.38,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 2.00,	# was 3.33,
	'toughness' : 2.67,
	'softness' : 0.45, #was 68,
	'air_defence' : 1.00,
	'armor_value' : 1,
	#Offensive Abilities,
	'suppression' : 1,
	'soft_attack' : 2.00,
	'hard_attack' : 0.33,
	'air_attack' : 0.10,
	'ap_attack' : 1,
   	'amphibious' :    { 'attack' : -0.40 },
    'river' :         { 'attack' : -0.05 },
	'urban' : {
		'attack' : -0.20,
		'defence' : -0.30,
		'movement' : 0.40
	},
   	'marsh' : 	{
        'attack' : -0.10 ,
		'defence' : -0.25,
		'movement' : -0.10
    },
	'jungle' : 	{
        'attack' : -0.30,
		'defence' : -0.25,
		'movement' : 0.05
    },
	'forest' : 	{
        'attack' : -0.20 ,
		'defence' : -0.15,
		'movement' : 0.10
    },
	'woods' : 	{
        'attack' : -0.05,
		'movement' : 0.15,
		'defence' : -0.10
    },
	'mountain' : 	{
        'attack' : -0.05 ,
		'movement' : 0.10,
		'defence' : -0.30
    },
	'hills' : 	{
		'defence' : -0.1,
		'movement' : 0.20
	},
	'plains' : {
		'movement' : 0.40
    },
	'desert' : {
		'movement' : 0.40
    },
	'arctic' : {
		'movement' : 0.20
    },
	'combat_width' : 0,
	'completion_size' : 0.4,
	'on_completion' : 'mobile_practical',
	'priority' : 4
},

'artillery_brigade' : {
	'type' : 'land',
	'sprite' : 'Infantry',
	'unit_group' : 'artillery_unit_type',
	#Size Definitions,
	'max_strength' : 10,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 5.00, #3,
	'build_cost_manpower' : 1.33,
	'build_time' : 100,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 4.00,
	'transport_weight' : 5.00,
	'supply_consumption' : 1.00,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 1.67,	# was 2.67,
	'toughness' : 1.00,
	'softness' : 0.90, #1.00,
	'air_defence' : 1.50,
	#Offensive Abilities,
	'suppression' : 0.00,
	'soft_attack' : 2.40,
	'hard_attack' : 1.00,
	'air_attack' : 0.10,
	'fort' : { 'attack' : 0.10 },
   	'amphibious' : { 'attack' : -0.80 },
   	'river' : { 'attack' : -0.05 },
	'urban' : {
		'attack' : -0.20
	},
   	'marsh' : {
        'attack' : -0.20,
		'movement' : -0.70
    },
	'jungle' : 	{
        'attack' : -0.30,
		'movement' : -0.30
    },
	'forest' : 	{
        'attack' : -0.20,
		'movement' : -0.20
    },
	'woods' : 	{
        'attack' : -0.10 ,
		'movement' : -0.10
    },
	'mountain' : 	{
        'attack' : -0.20,
		'movement' : -0.70
    },
	'hills' : 	{
		'attack' : -0.10,
        'movement' : -0.10
    },
	'combat_width' : 0,
	'completion_size' : 0.4,
	'on_completion' : 'artillery_practical',
	'priority' : 3
},

'assault_ship' : {
	'type' : 'naval',
	'sprite' : 'LSTlandingcraft',
	'radio_strength' : 1,
	'active' : 'no',
	'transport' : 'yes',
	'hull' : 1,
	'amphibious_invasion_speed' : 0.75,
	'amphibious_invasion_defence' : 0.25,
	'extra_amphibious_defence' : {
		'heavy_armor_brigade' : 0.25,
		'armor_brigade' : 0.25,
		'light_armor_brigade' : 0.25,
		'mechanized_brigade' : 0.25,
		'armored_car_brigade' : 0.25
	},
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 22,
	'default_morale' : 0.25,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' : 8.00,
	'build_cost_manpower' : 1.25,
	'build_time' : 110,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 18.00,
	'transport_capability' : 60.00,
	'supply_consumption' : 0.30,
	'fuel_consumption' : 1.25,
	'range' : 3500.00,
	'distance' : 0.10,
	#Detection Abilities,
	'surface_detection' : 0.00,
	'air_detection' : 0.00,
	'sub_detection' : 0.00,
	'visibility' : 80.00,
	#Defensive Abilities,
	'sea_defence' : 3.00,
	'air_defence' : 3.00,
	#Offensive Abilities,
	'convoy_attack' : 0.00,
	'sea_attack' : 0.00,
	'sub_attack' : 0.00,
	'air_attack' : 0.00,
	'shore_bombardment' : 0.00,
	'completion_size' : 1.0,
	'on_completion' : 'transport_practical',
	'priority' : 1
},

'battlecruiser' : {
	'type' : 'naval',
	'sprite' : 'Battlecruiser',
	'capital' : 'yes',
	'can_be_pride' : 'yes',
	'active' : 'no',
	'hull' : 1.3,
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' : 7.5,
	'build_cost_manpower' : 2.00,
	'build_time' : 480,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 20.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 0.50,
	'fuel_consumption' : 0.92,
	'range' : 2500.00,
	'distance' : 0.30,
	'radio_strength' : 1,
	#Detection Abilities,
	'surface_detection' : 1.00,
	'air_detection' : 1.00,
	'sub_detection' : 1.00,
	'visibility' : 80.00,
	#Defensive Abilities,
	'sea_defence' : 5.00,
	'air_defence' : 4.00,
	#Offensive Abilities,
	'convoy_attack' : 14.00,
	'sea_attack' : 12.00,
	'sub_attack' : 0.00,
	'air_attack' : 3.00,
	'shore_bombardment' : 7.00,
	'completion_size' : 3.7,
	'on_completion' : 'capitalship_practical',
	'priority' : 8
},

'battleship' : {
	'type' : 'naval',
	'sprite' : 'Battleship',
	'capital' : 'yes',
	'can_be_pride' : 'yes',
	'hull' : 1.5,
	'active' : 'no',
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' : 7.5,
	'build_cost_manpower' : 2.50,
	'build_time' : 585,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 18.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 0.70,
	'fuel_consumption' : 1.15,
	'range' : 2500.00,
	'distance' : 0.32,
	'radio_strength' : 1,
	#Detection Abilities,
	'surface_detection' : 1.00,
	'air_detection' : 1.00,
	'sub_detection' : 1.00,
	'visibility' : 90.00,
	#Defensive Abilities,
	'sea_defence' : 3.00,
	'air_defence' : 4.00,
	#Offensive Abilities,
	'convoy_attack' : 12.00,
	'sea_attack' : 15.00,
	'sub_attack' : 0.00,
	'air_attack' : 1.00,
	'shore_bombardment' : 10.00,
	'completion_size' : 4.4,
	'on_completion' : 'capitalship_practical',
	'priority' : 9
},

'bergsjaeger_brigade' : {
	'type' : 'land',
	'sprite' : 'Infantry',
	'active' : 'no',
	'unit_group' : 'infantry_unit_type',
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 40,
	'default_morale' : 0.40,
	'officers' : 130,
	#Building Costs,
	'build_cost_ic' : 2.67,
	'build_cost_manpower' : 4.00,
	'build_time' : 150,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 4.33,
	'transport_weight' : 8.00,
	'supply_consumption' : 0.67,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 4.00,	# was 5.33,
	'toughness' : 3.33,
	'softness' : 1.00,
	'air_defence' : 2.50,
	#Offensive Abilities,
	'suppression' : 0.67,
	'soft_attack' : 1.67,
	'hard_attack' : 0.33,
	'air_attack' : 0.10,
	'ap_attack' : 0.5,
	'mountain' : {
		'attack' : 0.50,
		'defence' : 0.20,
		'movement' : 0.10	
	},
    'hills' : {
		'attack' : 0.40,
		'defence' : 0.10,
		'movement' : 0.10
		},
	'arctic' : { 
		'attack' : 0.50,
		'defence' : 0.20,
		'movement' : 0.20	
		},
	'jungle' : 	{
        'attack' : 0.10,
		'movement' : 0.10
	},
	'forest' : 	{
        'attack' : 0.10,
		'movement' : 0.10
	},
	'woods' : 	{
        'attack' : 0.05,
		'movement' : 0.05
	}	,
	'combat_width' : 1,
	'completion_size' : 0.3,
	'on_completion' : 'infantry_practical',
	'priority' : 8
},

'cag' : {
	'type' : 'air',
	'active' : 'no',
	'sprite' : 'Fighter',
	'is_cag' : 'yes',
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' : 9.8,
	'build_cost_manpower' : 1.10,
	'build_time' : 180,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 300.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 1.10,
	'fuel_consumption' : 1.70,
	'range' : 450.00,
	'radio_strength' : 1,
	#Detection Abilities,
	'surface_detection' : 2.00,
	'air_detection' : 2.00,
	#Defensive Abilities,
	'surface_defence' : 3.00,
	'air_defence' : 6.00,
	#Offensive Abilities,
	'soft_attack' : 2.00,
	'hard_attack' : 2.00,
	'sea_attack' : 4.00,
	'air_attack' : 3.00,
	'strategic_attack' : 0.00,
	'sub_attack' : 4.00,
	'completion_size' : 1.8,
	'on_completion' : 'single_engine_aircraft_practical',
	'priority' : 5
},

'carrier' : {
	'type' : 'naval',
	'sprite' : 'Carrier',
	'capital' : 'yes',
	'active' : 'no',
	'hull' : 1.0,
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 0,
	'carrier_size' : 2,
	#Building Costs,
	'build_cost_ic' : 7.50,
	'build_cost_manpower' : 2.50,
	'build_time' : 500,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 20.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 1.00,
	'fuel_consumption' : 1.15,
	'range' : 2500.00,
	'distance' : 1.00,
	'radio_strength' : 1.25,
	#Detection Abilities,
	'surface_detection' : 1.00,
	'air_detection' : 2.00,
	'sub_detection' : 1.00,
	'visibility' : 100.00,
	#Defensive Abilities,
	'sea_defence' : 3.00,
	'air_defence' : 3.00,
	#Offensive Abilities,
	'convoy_attack' : 0.00,
	'sea_attack' : 0.00,
	'sub_attack' : 0.00,
	'air_attack' : 3.00,
	'shore_bombardment' : 0.00,
	'completion_size' : 3.4,
	'on_completion' : 'carrier_practical',
	'priority' : 10
},

'cas' : {
	'type' : 'air',
	'active' : 'no',
	'sprite' : 'Cas',
	'is_bomber' : 'yes',
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' : 11.00,
	'build_cost_manpower' : 1.30,
	'build_time' : 180,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 180.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 1.50,
	'fuel_consumption' : 2.30,
	'range' : 200.00,
	'radio_strength' : 1,
	#Detection Abilities,
	'surface_detection' : 1.00,
	'air_detection' : 0.00,
	#Defensive Abilities,
	'surface_defence' : 3.00,
	'air_defence' : 3.00,
	#Offensive Abilities,
	'soft_attack' : 3.00,
	'hard_attack' : 7.00,
	'sea_attack' : 7.00,
	'air_attack' : 2.00,
	'strategic_attack' : 0.00,
	'sub_attack' : 3.50,
	'completion_size' : 2.0,
	'on_completion' : 'single_engine_aircraft_practical',
	'priority' : 2
},

'cavalry_brigade' : {
	'type' : 'land',
	'sprite' : 'Cavalry',
	'unit_group' : 'infantry_unit_type',
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 2.00,
	'build_cost_manpower' : 3.00,
	'build_time' : 110,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 6.00,
	'transport_weight' : 15.00,
	'supply_consumption' : 1.00,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 2.00,	# was 3.33,
	'toughness' : 2.67,
	'softness' : 1.00,
	'air_defence' : 1.60,
	#Offensive Abilities,
	'suppression' : 1.00,
	'soft_attack' : 1.67,
	'hard_attack' : 0.00,
	'air_attack' : 0.33,
	'ap_attack' : 0.5,
	'amphibious' :    { 'attack' : -0.40 },
	'mountain' :      { 
        'attack' : -0.10,
	    'movement' : -0.05
	},
	'hills' :         { 'attack' : -0.05 },
   	'forest' :        { 'attack' : -0.05 },
	'jungle' :        { 'attack' : -0.10 },
	'combat_width' : 1,
	'completion_size' : 0.2,
	'on_completion' : 'mobile_practical',
	'priority' : 6,
},

'destroyer' : {
	'type' : 'naval',
	'sprite' : 'Destroyer',
	'active' : 'no',
	'hull' : 1.0,
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' : 4.50,
	'build_cost_manpower' : 1.00,
	'build_time' : 200,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 25.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 0.25,
	'fuel_consumption' : 0.28,
	'range' : 1500.00,
	'distance' : 0.14,
	'radio_strength' : 1,
	#Detection Abilities,
	'surface_detection' : 3.00,
	'air_detection' : 2.00,
	'sub_detection' : 7.00,
	'visibility' : 50.00,
	#Defensive Abilities,
	'sea_defence' : 15.00,
	'air_defence' : 3.00,
	#Offensive Abilities,
	'convoy_attack' : 2.00,
	'sea_attack' : 3.00,
	'sub_attack' : 5.00,
	'air_attack' : 3.00,
	'shore_bombardment' : 0.00,
	'completion_size' : 0.9,
	'on_completion' : 'destroyer_practical',
	'priority' : 4
},

'engineer_brigade' : {
	'type' : 'land',
	'sprite' : 'Infantry',
	'active' : 'no' ,
	'unit_group' : 'support_unit_type',
	#Size Definitions,
	'max_strength' : 10,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 3.33,
	'build_cost_manpower' : 1.33,
	'build_time' : 140,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 8.00,
	'transport_weight' : 8.00,
	'supply_consumption' : 0.67,
	'fuel_consumption' : 0.38,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 3.00,	# was 4.33,
	'toughness' : 1.67,
	'softness' : 0.90, #95,
	'air_defence' : 2.00,
	#Offensive Abilities,
	'suppression' : 0.33,
	'soft_attack' : 0.33,
	'hard_attack' : 0.67,
	'air_attack' : 0.00,
	'river' : {
		'attack' : 0.6,
		'defence' : 1.0
	},
	'fort' : {
		'attack' : 0.40
	},
	'amphibious' :    {
		 'attack' : 0.4
    },
	'marsh' : {
		'movement' : 0.2,
		'attack' : 0.2,
		'defence' : 0.3
	},
	'urban' : {
		'attack' : 0.4,
		'defence' : 0.6
	},
	'mountain' : {
        'defence' : 0.5
    },
	'hills' : {
        'defence' : 0.5
    },
	'plains' : {
        'defence' : 0.5
    },
	'desert' : {
        'defence' : 0.5
    },
	'arctic' : {
        'defence' : 0.5
    },
	'combat_width' : 0,
	'completion_size' : 0.3,
	'on_completion' : 'infantry_practical',
	'priority' : 2
},

'escort_carrier' : {
	'type' : 'naval',
	'sprite' : 'Carrier',
	'capital' : 'yes',
	'active' : 'no',
	'hull' : 0.8,
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 0,
	'carrier_size' : 1,
	#Building Costs,
	'build_cost_ic' : 4.50,
	'build_cost_manpower' : 2.00,
	'build_time' : 350,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 18.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 1.00,
	'fuel_consumption' : 0.92,
	'range' : 2000.00,
	'distance' : 1.00,
	'radio_strength' : 1.1,
	#Detection Abilities,
	'surface_detection' : 1.00,
	'air_detection' : 2.00,
	'sub_detection' : 1.00,
	'visibility' : 80.00,
	#Defensive Abilities,
	'sea_defence' : 5.00,
	'air_defence' : 1.50,
	#Offensive Abilities,
	'convoy_attack' : 0.00,
	'sea_attack' : 0.00,
	'sub_attack' : 0.00,
	'air_attack' : 1.00,
	'shore_bombardment' : 0.00,
	'completion_size' : 1.6,
	'on_completion' : 'carrier_practical',
	'priority' : 7
},

'flying_bomb' : {
	'type' : 'air',
	'active' : 'no',
	'is_bomber' : 'yes',
	'sprite' : 'Rocket',
	'is_rocket' : 'yes',
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 100,
	'default_morale' : 1.00,
	#Building Costs,
	'build_cost_ic' : 2.00,
	'build_cost_manpower' : 0.00,
	'build_time' : 30,
	'officers' : 0,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 650.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 0.50,
	'fuel_consumption' : 1.15,
	'range' : 300.00,
	'radio_strength' : 1,
	#Detection Abilities,
	'surface_detection' : 0.00,
	'air_detection' : 0.00,
	#Defensive Abilities,
	'surface_defence' : 30.00,
	'air_defence' : 15.00,
	#Offensive Abilities,
	'soft_attack' : 0.00,
	'hard_attack' : 0.00,
	'sea_attack' : 0.00,
	'air_attack' : 0.00,
	'strategic_attack' : 80.00,
	'sub_attack' : 0.00,
	'completion_size' : 0.1,
	'on_completion' : 'rocket_practical',
	'priority' : 8
},

'flying_rocket' : {
	'type' : 'air',
	'active' : 'no',
	'sprite' : 'Rocket',
	'is_bomber' : 'yes',
	'is_rocket' : 'yes',
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 100,
	'default_morale' : 1.00,
	#Building Costs,
	'build_cost_ic' : 3.00,
	'build_cost_manpower' : 0.00,
	'build_time' : 40,
	'officers' : 0,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 5000.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 0.50,
	'fuel_consumption' : 2.30,
	'range' : 380.00,
	'radio_strength' : 1,
	#Detection Abilities,
	'surface_detection' : 0.00,
	'air_detection' : 0.00,
	#Defensive Abilities,
	'surface_defence' : 100.00,
	'air_defence' : 100.00,
	#Offensive Abilities,
	'soft_attack' : 0.00,
	'hard_attack' : 0.00,
	'sea_attack' : 0.00,
	'air_attack' : 0.00,
	'strategic_attack' : 110.00,
	'sub_attack' : 0.00,
	'completion_size' : 0.1,
	'on_completion' : 'rocket_practical',
	'priority' : 10,
},

'garrison_brigade' : {
	'type' : 'land',
	'sprite' : 'Infantry',
	'unit_group' : 'infantry_unit_type',
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 30,
	'default_morale' : 0.15,
	'officers' : 30,
	#Building Costs,
	'build_cost_ic' : 1.67,
	'build_cost_manpower' : 2.67,
	'build_time' : 60,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 1.00,
	'transport_weight' :8.00,
	'supply_consumption' : 0.16,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 3.50,	# was 5.00,
	'toughness' : 0.67,
	'softness' : 1.00,
	'air_defence' : 2.00,
	#Offensive Abilities,
	'suppression' : 2.00,
	'soft_attack' : 1.67,
	'hard_attack' : 0.33,
	'air_attack' : 0.15,
	'ap_attack' : 0.5,
    'amphibious' : { 'attack' : -0.20 },
    'river' : { 'attack' : -0.10 },
	'combat_width' : 1,
	'completion_size' : 0.2,
	'on_completion' : 'militia_practical',
	'priority' : 5
},

'guards_brigade' : {
	'usable_by' :  'SOV' ,
	'type' : 'land',
	'sprite' : 'Elite',
	'unit_group' : 'infantry_unit_type',
	#lim'ITA'tions,
	'minimum_of_type' : 6 ,
	'max_percentage_of_type' : 0.04,
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 45,
	'default_morale' : 0.60,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 2.67,
	'build_cost_manpower' : 3.00,
	'build_time' : 120,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 4.00,
	'transport_weight' : 10.00,
	'supply_consumption' : 0.67,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 5.00,
	'toughness' : 4.00,
	'softness' : 1.00,
	'air_defence' : 2.00,
	#Offensive Abilities,
	'suppression' : 1.00,
	'soft_attack' : 2.00,
	'hard_attack' : 0.67,
	'air_attack' : 0.33,
	'ap_attack' : 1,
	'combat_width' : 1,
	'completion_size' : 0.2,
	'on_completion' : 'infantry_practical',
	'priority' : 9
	},

'gurkha_brigade' : {
	'usable_by' : [ 'ENG' ,'NEP' ],
	'type' : 'land',
	'sprite' : 'Elite',
	'unit_group' : 'infantry_unit_type',
	'can_paradrop' : 'yes',
	#lim'ITA'tions,
	'minimum_of_type' : 6 ,
	'max_percentage_of_type' : 0.04,
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 50,
	'default_morale' : 0.45,
	'officers' : 130,
	#Building Costs,
	'build_cost_ic' : 2.67,
	'build_cost_manpower' : 4.00,
	'build_time' : 150,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 4.33,
	'transport_weight' : 5.00,
	'supply_consumption' : 0.67,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 4.00,
	'toughness' : 3.33,
	'softness' : 1.00,
	'air_defence' : 2.50,
	#Offensive Abilities,
	'suppression' : 0.67,
	'soft_attack' : 1.67,
	'hard_attack' : 0.33,
	'air_attack' : 0.10,
	'ap_attack' : 1,
	'mountain' : {
		'attack' : 0.60,
		'defence' : 0.20,
		'movement' : 0.15	
	},
    'hills' : {
		'attack' : 0.50,
		'defence' : 0.10,
		'movement' : 0.15
		},
	'arctic' : { 
		'attack' : 0.50,
		'defence' : 0.20,
		'movement' : 0.20	
		},
	'jungle' : 	{
        'attack' : 0.20,
		'movement' : 0.10
	},
	'forest' : 	{
        'attack' : 0.15,
		'movement' : 0.10
	},
	'woods' : 	{
        'attack' : 0.1,
		'movement' : 0.05
	}	,
	'combat_width' : 1,
	'completion_size' : 0.3,
	'on_completion' : 'infantry_practical',
	'priority' : 9
},

'heavy_armor_brigade' : {
	'type' : 'land',
	'sprite' : 'Tank',
	'active' : 'no',
	'unit_group' : 'armor_unit_type',
	'is_armor' : 'yes',
	'is_mobile' : 'yes',
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 20.00,
	'build_cost_manpower' : 2.33,
	'build_time' : 210,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 5.00,
	'transport_weight' : 20.00,
	'supply_consumption' : 2.0,
	'fuel_consumption' : 4.60,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 5,		# was 7,
	'toughness' : 4,
	'softness' : 0.10,
	'air_defence' : 0.60,
	'armor_value' : 8,
	#Offensive Abilities,
	'suppression' : 0.00,
	'soft_attack' : 4.33,
	'hard_attack' : 5,
	'air_attack' : 0.33,
	'ap_attack' : 8,
   	'amphibious' :    { 'attack' : -1.8 },
   	'river' :         { 'attack' : -0.35 },
	'urban' : {
		'attack' : -0.50,
		'defence' : -0.80
	},
   	'marsh' : {
        'attack' : -0.40 ,
		'movement' : -0.90,
		'defence' : -0.70
    },
	'jungle' : {
        'attack' : -0.60,
		'movement' : -0.50,
		'defence' : -0.70
	},
	'forest' : {
        'attack' : -0.40,
		'movement' : -0.50,
		'defence' : -0.50
	},
	'woods' : 	{
        'attack' : -0.10,
		'movement' : -0.20,
		'defence' : -0.40
    },
	'mountain' : 	{
        'attack' : -0.45,
		'movement' : -0.50,
		'defence' : -0.80
	},
	'hills' :         {
        'attack' : -0.20   ,
		'movement' : -0.20,
		'defence' : -0.40
    },
	'combat_width' : 2,
	'completion_size' : 3,
	'on_completion' : 'armour_practical',
	'priority' : 10,
},

'heavy_cruiser' : {
	'type' : 'naval',
	'sprite' : 'HeavyCruiser',
	'active' : 'no',
	'capital' : 'yes',
	'hull' : 1.1,
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' : 6.00,
	'build_cost_manpower' : 1.50,
	'build_time' : 280,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 20.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 0.40,
	'fuel_consumption' : 0.58,
	'range' : 2500.00,
	'distance' : 0.24,
	'radio_strength' : 1,
	#Detection Abilities,
	'surface_detection' : 1.00,
	'air_detection' : 1.00,
	'sub_detection' : 2.00,
	'visibility' : 70.00,
	#Defensive Abilities,
	'sea_defence' : 8.00,
	'air_defence' : 4.00,
	#Offensive Abilities,
	'convoy_attack' : 6.00,
	'sea_attack' : 8.00,
	'sub_attack' : 1.00,
	'air_attack' : 6.00,
	'shore_bombardment' : 3.00,
	'completion_size' : 1.6,
	'on_completion' : 'cruiser_practical',
	'priority' : 6
},

'hq_brigade' : {
	'type' : 'land',
	'sprite' : 'Infantry',
	'is_buildable' : 'no',
	#Size Definitions,
	'max_strength' : 10,
	'default_organisation' : 30,
	'default_morale' : 0.40,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 2.00,
	'build_cost_manpower' : 1.67,
	'build_time' : 120,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 3.00,
	'transport_weight' : 10.00,
	'supply_consumption' : 0.67,
	'fuel_consumption' : 0.00,
	'radio_strength' : 2,
	#Defensive Abilities,
	'defensiveness' : 4.33,	# was 5.67,
	'toughness' : 2.67,
	'softness' : 1.00,
	'air_defence' : 2.00,
	#Offensive Abilities,
	'suppression' : 0.67,
	'soft_attack' : 0.33,
	'hard_attack' : 0.00,
	'air_attack' : 0.33,
   	'amphibious' : { 'attack' : -0.10 },
    'river' : { 'attack' : -0.05 },
	'combat_width' : 0,		# was 1,
	'completion_size' : 0.2,
	'on_completion' : 'land_doctrine_practical',
	'priority' : 11
},

'imperial_brigade' : {
	'usable_by' :  'JAP' ,
	'type' : 'land',
	'sprite' : 'Elite',
	'unit_group' : 'infantry_unit_type',
	#lim'ITA'tions,
	'minimum_of_type' : 6 ,
	'max_percentage_of_type' : 0.04,
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 50,
	'default_morale' : 0.50,
	'officers' : 150,
	#Building Costs,
	'build_cost_ic' : 2.67,
	'build_cost_manpower' : 4.00,
	'build_time' : 175,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 4.00,
	'transport_weight' : 10.00,
	'supply_consumption' : 0.67,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 4.00,	# 5.33,
	'toughness' : 3.00,
	'softness' : 1.00,
	'air_defence' : 2.00,
	#Offensive Abilities,
	'suppression' : 1.00,
	'soft_attack' : 2.00,
	'hard_attack' : 0.67,
	'air_attack' : 0.33,
	'ap_attack' : 1,
	'hills' : {
		'attack' : 0.05,
		'movement' : 0.05
		},
	'jungle' : {
        'attack' : 0.20,
		'movement' : 0.15
	},
	'forest' : {
        'attack' : 0.15,
		'movement' : 0.15
	},
	'woods' : {
        'attack' : 0.15,
		'movement' : 0.15
	}	,
	'amphibious' : { 
		'attack' : 0.10 
	},
    'river' : { 
		'attack' : 0.05 
	},
	'night' : {
		'attack' : 0.10,
		'defence' : 0.10,
		'movement' : 0.10	
	},
	'combat_width' : 1,
	'completion_size' : 0.2,
	'on_completion' : 'infantry_practical',
	'priority' : 9
},

'infantry_brigade' : {
	'type' : 'land',
	'sprite' : 'Infantry',
	'active' : 'no',
	'unit_group' : 'infantry_unit_type',
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 2.33,
	'build_cost_manpower' : 3.33,
	'build_time' : 95,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 4.00,
	'transport_weight' : 10.00,
	'supply_consumption' : 0.67,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 4.00,	# 5.33,
	'toughness' : 3.00,
	'softness' : 1.00,
	'air_defence' : 2.00,
	#Offensive Abilities,
	'suppression' : 1.00,
	'soft_attack' : 2.00,
	'hard_attack' : 0.67,
	'air_attack' : 0.33,
	'ap_attack' : 1,
	'combat_width' : 1,
	'completion_size' : 0.2,
	'on_completion' : 'infantry_practical',
	'priority' : 7
},

'interceptor' : {
	'type' : 'air',
	'active' : 'no',
	'sprite' : 'Fighter',
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' : 14.00,
	'build_cost_manpower' : 1.00,
	'build_time' : 140,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 450.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 0.70,
	'fuel_consumption' : 1.70,
	'range' : 220.00,
	'radio_strength' : 1,
	#Detection Abilities,
	'surface_detection' : 1.00,
	'air_detection' : 1.00,
	#Defensive Abilities,
	'surface_defence' : 0.00,
	'air_defence' : 5.50,
	#Offensive Abilities,
	'soft_attack' : 1.00,
	'hard_attack' : 0.00,
	'sea_attack' : 0.00,
	'air_attack' : 4.00,
	'strategic_attack' : 0.00,
	'sub_attack' : 0.00,
	'completion_size' : 2.0,
	'on_completion' : 'single_engine_aircraft_practical',
	'priority' : 5
},

'invasion_transport_ship' : {
	'type' : 'naval',
	'sprite' : 'LCTlandingcraft',
	'radio_strength' : 1,
	'active' : 'no',
	'transport' : 'yes',
	'hull' : 1,
	'amphibious_invasion_speed' : 0.50, # added to base,
	'amphibious_invasion_defence' : 0.10,
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 22,
	'default_morale' : 0.25,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' : 6.00,
	'build_cost_manpower' : 1.25,
	'build_time' : 110,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 18.00,
	'transport_capability' : 60.00,
	'supply_consumption' : 0.30,
	'fuel_consumption' : 1.25,
	'range' : 3500.00,
	'distance' : 0.10,
	#Detection Abilities,
	'surface_detection' : 0.00,
	'air_detection' : 0.00,
	'sub_detection' : 0.00,
	'visibility' : 80.00,
	#Defensive Abilities,
	'sea_defence' : 1.00,
	'air_defence' : 1.00,
	#Offensive Abilities,
	'convoy_attack' : 0.00,
	'sea_attack' : 0.00,
	'sub_attack' : 0.00,
	'air_attack' : 0.00,
	'shore_bombardment' : 0.00,
	'completion_size' : 1.0,
	'on_completion' : 'transport_practical',
	'priority' : 1
},

'light_armor_brigade' : {
	'type' : 'land',
	'sprite' : 'Tank',
	'active' : 'no',
	'unit_group' : 'armor_unit_type',
	'is_mobile' : 'yes',
	'is_armor' : 'yes',
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 8.2,
	'build_cost_manpower' : 2.33,
	'build_time' : 160,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 8.00,
	'transport_weight' : 15.00,
	'supply_consumption' : 1.33,
	'fuel_consumption' : 2.30,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 2.33,	# was 3.67,
	'toughness' : 3.33,
	'softness' : 0.30,
	'air_defence' : 1.00,
	'armor_value' : 2,
	#Offensive Abilities,
	'suppression' : 0.33,
	'soft_attack' : 1.67,
	'hard_attack' : 0.67,
	'air_attack' : 0.33,
	'ap_attack' : 2,
   	'amphibious' :    { 'attack' : -0.60 },
   	'river' :         { 'attack' : -0.05 },
	'urban' : {
		'attack' : -0.40,
		'defence' : -0.60
	},
   	'marsh' : 	{
        'attack' : -0.30 ,
		'movement' : -0.70,
		'defence' : -0.5
    },
	'jungle' : 	{
        'attack' : -0.30 ,
		'movement' : -0.30,
		'defence' : -0.5
	},
	'forest' : 	{
        'attack' : -0.20 ,
		'movement' : -0.30,
		'defence' : -0.3
	},
	'woods' : 	{
        'attack' : -0.05 ,
		'movement' : -0.10,
		'defence' : -0.2
    },
	'mountain' : 	{
        'attack' : -0.20 ,
	    'movement' : -0.30,
		'defence' : -0.60
    },
	'hills' : {
        'attack' : -0.05   ,
		'movement' : -0.05,
		'defence' : -0.2
    },
	'combat_width' : 2,
	'completion_size' : 1.2,
	'on_completion' : 'armour_practical',
	'priority' : 10
},

'light_cruiser' : {
	'type' : 'naval',
	'sprite' : 'LightCruiser',
	'active' : 'no',
	'hull' : 1,
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' : 4.50,
	'build_cost_manpower' : 1.00,
	'build_time' : 280,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 23.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 0.36,
	'fuel_consumption' : 0.43,
	'range' : 2000.00,
	'distance' : 0.18,
	'radio_strength' : 1,
	#Detection Abilities,
	'surface_detection' : 2.00,
	'air_detection' : 2.00,
	'sub_detection' : 3.00,
	'visibility' : 60.00,
	#Defensive Abilities,
	'sea_defence' : 12.00,
	'air_defence' : 5.00,
	#Offensive Abilities,
	'convoy_attack' : 3.00,
	'sea_attack' : 5.00,
	'sub_attack' : 2.00,
	'air_attack' : 5.00,
	'shore_bombardment' : 1.00,
	'completion_size' : 1.2,
	'on_completion' : 'cruiser_practical',
	'priority' : 5
},

'marine_brigade' : {
	'type' : 'land',
	'sprite' : 'Infantry',
	'active' : 'no',
	'unit_group' : 'infantry_unit_type',
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 40,
	'default_morale' : 0.40,
	'officers' : 130,
	#Building Costs,
	'build_cost_ic' : 3.00,
	'build_cost_manpower' : 4.00,
	'build_time' : 160,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 4.33,
	'transport_weight' : 10.00,
	'supply_consumption' : 0.80,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 3.50,	# was 5.33,
	'toughness' : 3.67,
	'softness' : 1.00,
	'air_defence' : 2.50,
	#Offensive Abilities,
	'suppression' : 0.67,
	'soft_attack' : 1.67,
	'hard_attack' : 0.33,
	'air_attack' : 0.33,
	'ap_attack' : 0.5,
	'amphibious' : {
		'attack' : 0.50
	},
	'river' : {
		'attack' : 0.40
	},
	'marsh' : {
		'attack' : 0.3
	},
	'jungle' : {
        'attack' : 0.25,
		'defence' : 0.25,
		'movement' : 0.25
    },
	'combat_width' : 1,
	'completion_size' : 0.3,
	'on_completion' : 'infantry_practical',
	'priority' : 8,
},

'mechanized_brigade' : {
	'type' : 'land',
	'sprite' : 'Mech',
	'active' : 'no',
	'unit_group' : 'infantry_unit_type',
	'is_mobile' : 'yes',
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 7.50,
	'build_cost_manpower' : 3.00,
	'build_time' : 160,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 9.00,
	'transport_weight' : 15.00,
	'supply_consumption' : 1.33,
	'fuel_consumption' : 2.3,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 4.33,	# 5.67,
	'toughness' : 4.00,
	'softness' : 0.60, #75,
	'air_defence' : 1.50,
	#Offensive Abilities,
	'suppression' : 1.00,
	'soft_attack' : 3.00,
	'hard_attack' : 1.00,
	'air_attack' : 0.33,
	'ap_attack' : 1,
   	'amphibious' :    { 'attack' : -0.40 },
   	'river' :         { 'attack' : -0.05 },
   	'marsh' : 	{
        'attack' : -0.10 ,
		'movement' : -0.70,
		'defence' : -0.2
    },
	'jungle' : 	{
        'attack' : -0.30 ,
		'defence' : -0.2,
		'movement' : -0.30
	},
	'forest' : 	{
        'attack' : -0.10 ,
		'movement' : -0.30
	},
	'woods' : 	{
        'attack' : -0.05 ,
		'movement' : -0.10
    },
	'mountain' : 	{
        'attack' : -0.10 ,
		'movement' : -0.30,
		'defence' : -0.3
    },
	'hills' : {
        'attack' : -0.05   ,
		'movement' : -0.05
    },
	'urban' : {
		'attack' : -0.20,
		'defence' : -0.05
	},
	'combat_width' : 1,
	'completion_size' : 1.2,
	'on_completion' : 'mobile_practical',
	'priority' : 9
},

'militia_brigade' : {
	'type' : 'land',
	'sprite' : 'Infantry',
	#'unit_group' : 'infantry_unit_type' #'no' CA for you,
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 20,
	'default_morale' : 0.20,
	'officers' : 10,
	#Building Costs,
	'build_cost_ic' : 1.33,
	'build_cost_manpower' : 2.00,
	'build_time' : 50,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 3.00,
	'transport_weight' : 5.00,
	'supply_consumption' : 0.20,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 3.00,	# was 4.33,
	'toughness' : 0.67,
	'softness' : 1.00,
	'air_defence' : 2.00,
	#Offensive Abilities,
	'suppression' : 0.33,
	'soft_attack' : 0.67,
	'hard_attack' : 0.00,
	'air_attack' : 0.00,
   	'amphibious' : { 'attack' : -0.20 },
    'river' : { 'attack' : -0.10 },
	'urban' : {
        'attack' : 0.05,
		'defence' : 0.15
    },
    'marsh' : { 'defence' : 0.10 },
    'jungle' : { 'defence' : 0.15 },
    'forest' : { 'defence' : 0.10 },
    'woods' : { 'defence' : 0.05 },
    'mountain' : { 'defence' : 0.10 },
    'hills' : { 'defence' : 0.05 },
	'combat_width' : 1,
	'completion_size' : 0.1,
	'on_completion' : 'militia_practical',
	'priority' : 6
},

'militia_brigade' : {
	'type' : 'land',
	'sprite' : 'Infantry',
	#'unit_group' : 'infantry_unit_type' #'no' CA for you,
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 20,
	'default_morale' : 0.20,
	'officers' : 10,
	#Building Costs,
	'build_cost_ic' : 1.33,
	'build_cost_manpower' : 2.00,
	'build_time' : 50,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 3.00,
	'transport_weight' : 5.00,
	'supply_consumption' : 0.20,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 3.00,	# was 4.33,
	'toughness' : 0.67,
	'softness' : 1.00,
	'air_defence' : 2.00,
	#Offensive Abilities,
	'suppression' : 0.33,
	'soft_attack' : 0.67,
	'hard_attack' : 0.00,
	'air_attack' : 0.00,
   	'amphibious' : { 'attack' : -0.20 },
    'river' : { 'attack' : -0.10 },
	'urban' : {
        'attack' : 0.05,
		'defence' : 0.15
    },
    'marsh' : { 'defence' : 0.10 },
    'jungle' : { 'defence' : 0.15 },
    'forest' : { 'defence' : 0.10 },
    'woods' : { 'defence' : 0.05 },
    'mountain' : { 'defence' : 0.10 },
    'hills' : { 'defence' : 0.05 },
	'combat_width' : 1,
	'completion_size' : 0.1,
	'on_completion' : 'militia_practical',
	'priority' : 6
},

'mot_aa_brigade' : {
	'type' : 'land',
	'sprite' : 'Motor',
	'unit_group' : 'direct_fire_unit_type',
	'active' : 'no',
	'is_mobile' : 'yes',
	#Size Definitions,
	'max_strength' : 10,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 3,
	'build_cost_manpower' : 1.67,
	'build_time' : 120,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 8.00,
	'transport_weight' : 8.00,
	'supply_consumption' : 0.75,
	'fuel_consumption' : 0.75,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 2.00,
	'toughness' : 1.00,
	'softness' : 0.80 ,#90,
	'air_defence' : 5.00,
	#Offensive Abilities,
	'suppression' : 0.00,
	'soft_attack' : 0.70,
	'hard_attack' : 1.00,
	'air_attack' : 6.00,
   	'amphibious' : { 'attack' : -0.20 },
   	'river' : { 'attack' : -0.05 },
	'urban' : {
		'attack' : -0.20
	},
   	'marsh' : {
        'attack' : -0.20,
		'movement' : -0.70
    },
	'jungle' : 	{
        'attack' : -0.30,
		'movement' : -0.30
    },
	'forest' : 	{
        'attack' : -0.20,
		'movement' : -0.20
    },
	'woods' : 	{
        'attack' : -0.10 ,
		'movement' : -0.10
    },
	'mountain' : 	{
        'attack' : -0.20,
		'movement' : -0.70
    },
	'hills' : 	{
		'attack' : -0.10,
        'movement' : -0.10
    },
	'combat_width' : 0,
	'completion_size' : 0.3,
	'on_completion' : 'artillery_practical',
	'priority' : 3
},

'motorized_brigade' : {
	'type' : 'land',
	'sprite' : 'Motor',
	'active' : 'no',
	'unit_group' : 'infantry_unit_type',
	'is_mobile' : 'yes',
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 4.67,
	'build_cost_manpower' : 3.33,
	'build_time' : 120,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 8.00,
	'transport_weight' : 15.00,
	'supply_consumption' : 1.00,
	'fuel_consumption' : 1.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 3.50,	# was 5.33,
	'toughness' : 3.00,
	'softness' : 0.70, #90,
	'air_defence' : 1.50,
	#Offensive Abilities,
	'suppression' : 0.67,
	'soft_attack' : 2.00,
	'hard_attack' : 0.67,
	'air_attack' : 0.33,
	'ap_attack' : 1,
   	'amphibious' :    { 'attack' : -0.20 },
	'urban' : {
		'attack' : -0.10
	},
   	'marsh' : 	{
        'attack' : -0.10 ,
		'movement' : -0.80
    },
	'jungle' : 	{
        'attack' : -0.20,
		'movement' : -0.40
    },
	'forest' : 	{
        'attack' : -0.10,
		'movement' : -0.30
    },
	'woods' : 	{
        'attack' : -0.05 ,
		'movement' : -0.20
    },
	'mountain' : 	{
        'attack' : -0.20 ,
		'movement' : -0.40
    },
	'combat_width' : 1,
	'completion_size' : 0.6,
	'on_completion' : 'mobile_practical',
	'priority' : 9
},

'multi_role' : {
	'type' : 'air',
	'active' : 'no',
	'sprite' : 'Fighter',
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' : 15.00,
	'build_cost_manpower' : 1.00,
	'build_time' : 150,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 350.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 1.10,
	'fuel_consumption' : 2.07,
	'range' : 400.00,
	'radio_strength' : 1,
	#Detection Abilities,
	'surface_detection' : 1.00,
	'air_detection' : 3.00,
	#Defensive Abilities,
	'surface_defence' : 2.00,
	'air_defence' : 7.00,
	#Offensive Abilities,
	'soft_attack' : 2.00,
	'hard_attack' : 1.00,
	'sea_attack' : 1.00,
	'air_attack' : 5.00,
	'strategic_attack' : 0.00,
	'sub_attack' : 0.00,
	'completion_size' : 2.1,
	'on_completion' : 'single_engine_aircraft_practical',
	'priority' : 5,
},

'naval_bomber' : {
	'type' : 'air',
	'active' : 'no',
	'sprite' : 'naval',
	'is_bomber' : 'yes',
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' : 14.00,
	'build_cost_manpower' : 1.60,
	'build_time' : 220,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 200.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 2.00,
	'fuel_consumption' : 3.45,
	'range' : 400.00,
	'radio_strength' : 1,
	#Detection Abilities,
	'surface_detection' : 4.00,
	'air_detection' : 1.00,
	#Defensive Abilities,
	'surface_defence' : 5.00,
	'air_defence' : 5.00,
	#Offensive Abilities,
	'soft_attack' : 5.00,
	'hard_attack' : 2.00,
	'sea_attack' : 7.00,
	'air_attack' : 2.00,
	'strategic_attack' : 2.00,
	'sub_attack' : 7.00,
	'completion_size' : 3.1,
	'on_completion' : 'twin_engine_aircraft_practical',
	'priority' : 3
},

'nuclear_submarine' : {
	'type' : 'naval',
	'sprite' : 'Submarine',
	'is_sub' : 'yes',
	'active' : 'no',
	'hull' : 1,
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' : 8.00,
	'build_cost_manpower' : 0.50,
	'build_time' : 320,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 25.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 1.90,
	'fuel_consumption' : 0.00,
	'range' : 8000.00,
	'distance' : 0.20,
	'radio_strength' : 1,
	#Detection Abilities,
	'surface_detection' : 7.00,
	'air_detection' : 1.00,
	'sub_detection' : 5.00,
	'visibility' : 1.00,
	#Defensive Abilities,
	'sea_defence' : 13.00,
	'air_defence' : 16.00,
	#Offensive Abilities,
	'convoy_attack' : 14.00,
	'sea_attack' : 7.00,
	'sub_attack' : 15.00,
	'air_attack' : 1.00,
	'shore_bombardment' : 0.00,
	'completion_size' : 1.8,
	'on_completion' : 'submarine_practical',
	'priority' : 3,
},

'paratrooper_brigade' : {
	'type' : 'land',
	'sprite' : 'Infantry',
	'active' : 'no',
	'can_paradrop' : 'yes',
	'unit_group' : 'infantry_unit_type',
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 40,
	'default_morale' : 0.40,
	'officers' : 150,
	#Building Costs,
	'build_cost_ic' : 4.00,
	'build_cost_manpower' : 4.00,
	'build_time' : 150,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 4.33,
	'transport_weight' : 5.00,
	'supply_consumption' : 0.50,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 4.00,	# was 6.00,
	'toughness' : 4.00,
	'softness' : 1.00,
	'air_defence' : 2.50,
	#Offensive Abilities,
	'suppression' : 0.67,
	'soft_attack' : 1.67,
	'hard_attack' : 0.20,
	'air_attack' : 0.10,
	'ap_attack' : 0.5,
	'combat_width' : 1,
	'completion_size' : 0.4,
	'on_completion' : 'infantry_practical',
	'priority' : 8,
},

'partisan_brigade' : {
	'type' : 'land',
	'sprite' : 'Infantry',
	'is_buildable' : 'no',
	'unit_group' : 'infantry_unit_type',
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 20,
	'default_morale' : 0.20,
	'officers' : 10,
	#Building Costs,
	'build_cost_ic' : 1.33,
	'build_cost_manpower' : 2.00,
	'build_time' : 50,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 3.00,
	'transport_weight' : 5.00,
	'supply_consumption' : 0.0,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 3.00,	# was 4.33,
	'toughness' : 0.67,
	'softness' : 1.00,
	'air_defence' : 1.00,
	#Offensive Abilities,
	'suppression' : 0.33,
	'soft_attack' : 0.67,
	'hard_attack' : 0.00,
	'air_attack' : 0.00,
	'ap_attack' : 0.5,
   	'amphibious' : { 'attack' : -0.25 },	# -0.05 from militia,
    'river' : { 'attack' : -0.15 }	,	# -0.05 from militia,
	'urban' : {
#        'attack' : 0.05		,
		'defence' : 0.20				# + 0.05 from militia,
    },
    'marsh' : { 'defence' : 0.15 }	,	# + 0.05 from militia,
    'jungle' : { 'defence' : 0.20 }	,	# + 0.05 from militia,
    'forest' : { 'defence' : 0.15 }	,	# + 0.05 from militia,
    'woods' : { 'defence' : 0.10 }	,	# + 0.05 from militia,
    'mountain' : { 'defence' : 0.15 },	# + 0.05 from militia,
    'hills' : { 'defence' : 0.10 }	,	# + 0.05 from militia,
	'combat_width' : 1,
	'completion_size' : 0.1,
	'on_completion' : 'militia_practical',
	'priority' : 6
},

'police_brigade' : {
	'type' : 'land',
	'sprite' : 'Infantry',
	#Size Definitions,
	'max_strength' : 10,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 50,
	#Building Costs,
	'build_cost_ic' : 3.00,
	'build_cost_manpower' : 0.75,
	'build_time' : 60,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 4.00,
	'transport_weight' : 5.00,
	'supply_consumption' : 0.30,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 3.00,	# was 4.33,
	'toughness' : 1.00,
	'softness' : 1.00,
	'air_defence' : 2.00,
	#Offensive Abilities,
	'suppression' : 3.00,
	'soft_attack' : 0.67,
	'hard_attack' : 0.00,
	'air_attack' : 0.00,
   	'amphibious' : { 'attack' : -0.10 },
	'urban' : { 'attack' : 0.05 },
	'combat_width' : 0,
	'completion_size' : 0.2,
	'on_completion' : 'militia_practical',
	'priority' : 2
},

'ranger_brigade' : {
	'usable_by' :  'USA' ,
	'type' : 'land',
	'sprite' : 'Elite',
	'unit_group' : 'infantry_unit_type',
	'can_paradrop' : 'yes',
	#lim'ITA'tions,
	'minimum_of_type' : 6 ,
	'max_percentage_of_type' : 0.04,
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 40,
	'default_morale' : 0.40,
	'officers' : 130,
	#Building Costs,
	'build_cost_ic' : 2.67,
	'build_cost_manpower' : 4.00,
	'build_time' : 150,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 4.00,
	'transport_weight' : 5.00,
	'supply_consumption' : 0.67,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 4.00,
	'toughness' : 3.00,
	'softness' : 1.00,
	'air_defence' : 2.00,
	#Offensive Abilities,
	'suppression' : 1.00,
	'soft_attack' : 2.00,
	'hard_attack' : 0.67,
	'air_attack' : 0.33,
	'ap_attack' : 1,
	'hills' : {
		'attack' : 0.05,
		'movement' : 0.05
		},
	'jungle' : {
        'attack' : 0.10,
		'movement' : 0.15
	},
	'forest' : {
        'attack' : 0.10,
		'movement' : 0.15
	},
	'woods' : {
        'attack' : 0.10,
		'movement' : 0.15
	},
	'night' : {
		'attack' : 0.10,
		'defence' : 0.10,
		'movement' : 0.10	
	},
	'urban' : {
        'attack' : 0.20,
        'defence' : 0.15
    },
	'combat_width' : 1,
	'completion_size' : 0.2,
	'on_completion' : 'infantry_practical',
	'priority' : 9
},

'rocket_artillery_brigade' : {
	'type' : 'land',
	'sprite' : 'Infantry',
	'active' : 'no',
	'unit_group' : 'artillery_unit_type',
	#Size Definitions,
	'max_strength' : 10,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 4,
	'build_cost_manpower' : 1.33,
	'build_time' : 95,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 5.00,
	'transport_weight' : 5.00,
	'supply_consumption' : 1.50,
	'fuel_consumption' : 0.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 1.50,	# was 2.67,
	'toughness' : 1.67,
	'softness' : 0.90, #1.00,
	'air_defence' : 1.50,
	#Offensive Abilities,
	'suppression' : 0.00,
	'soft_attack' : 2.67,
	'hard_attack' : 1.00,
	'air_attack' : 0.10,
	'fort' : { 'attack' : 0.10},
   	'amphibious' : { 'attack' : -0.60 },
   	'river' : { 'attack' : -0.05 },
	'urban' : {
		'attack' : -0.15
	},
   	'marsh' : {
        'attack' : -0.15,
		'movement' : -0.50
    },
	'jungle' : 	{
        'attack' : -0.15,
		'movement' : -0.20
    },
	'forest' : 	{
        'attack' : -0.15,
		'movement' : -0.15
    },
	'woods' : 	{
        'attack' : -0.05,
		'movement' : -0.05
    },
	'mountain' : 	{
        'attack' : -0.15,
		'movement' : -0.50
    },
	'hills' : 	{
		'attack' : -0.05,
        'movement' : -0.05
    },
	'combat_width' : 0,
	'completion_size' : 0.35,
	'on_completion' : 'artillery_practical',
	'priority' : 3
},

'rocket_interceptor' : {
	'type' : 'air',
	'active' : 'no',
	'sprite' : 'Fighter',
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' : 20.00,
	'build_cost_manpower' : 1.10,
	'build_time' : 150,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 1000.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 1.3,
	'fuel_consumption' : 4.0,
	'range' : 150.00,
	'radio_strength' : 1,
	#Detection Abilities,
	'surface_detection' : 1.00,
	'air_detection' : 6.00,
	#Defensive Abilities,
	'surface_defence' : 1.00,
	'air_defence' : 12.00,
	#Offensive Abilities,
	'soft_attack' : 2.00,
	'hard_attack' : 1.00,
	'sea_attack' : 1.00,
	'air_attack' : 10.00,
	'strategic_attack' : 0.00,
	'sub_attack' : 0.00,
	'completion_size' : 2.1,
	'on_completion' : 'jetengine_practical',
	'priority' : 5
},

'sp_artillery_brigade' : {
	'type' : 'land',
	'sprite' : 'Infantry',
	'active' : 'no',
	'unit_group' : 'artillery_unit_type',
	'is_mobile' : 'yes',
	#Size Definitions,
	'max_strength' : 10,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 7.50, #5,
	'build_cost_manpower' : 1.33,
	'build_time' : 160,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 7.00,
	'transport_weight' : 8.00,
	'supply_consumption' : 1.00,
	'fuel_consumption' : 1.15,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 3.00,	# was 5.00,
	'toughness' : 3.00,
	'softness' : 0.70,
	'air_defence' : 1.00,
	#Offensive Abilities,
	'suppression' : 0.00,
	'soft_attack' : 2.67,
	'hard_attack' : 1.33,
	'air_attack' : 0.10,
   	'amphibious' :    { 'attack' : -0.80 },
	'fort' : 		{'attack' : 0.10},
	'urban' : {
		'attack' : -0.20,
		'defence' : -0.30
	},
   	'marsh' : 	{
        'attack' : -0.20,
	    'movement' : -0.70,
		'defence' : -0.25
	},
	'jungle' : 	{
        'attack' : -0.30,
	    'movement' : -0.40,
		'defence' : -0.25
	},
	'forest' : 	{
        'attack' : -0.20,
	    'movement' : -0.30,
		'defence' : -0.15
	},
	'woods' : 	{
        'attack' : -0.10,
		'movement' : -0.10,
		'defence' : -0.10
    },
	'mountain' : 	{
        'attack' : -0.20,
		'movement' : -0.50,
		'defence' : -0.30
    },
	'hills' : 	{
        'movement' : -0.10,
		'attack' : -0.10,
		'defence' : -0.10
    },
	'combat_width' : 0,
	'completion_size' : 0.9,
	'on_completion' : 'artillery_practical',
	'priority' : 4
},

'sp_rct_artillery_brigade' : {
	'type' : 'land',
	'sprite' : 'Infantry',
	'active' : 'no',
	'is_mobile' : 'yes',
	'unit_group' : 'artillery_unit_type',
	#Size Definitions,
	'max_strength' : 10,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 6,
	'build_cost_manpower' : 1.33,
	'build_time' : 160,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 8.00,
	'transport_weight' : 8.00,
	'supply_consumption' : 1.50,
	'fuel_consumption' : 0.8,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 2.33,	# was 3.66,
	'toughness' : 3.00,
	'softness' : 0.70,
	'air_defence' : 1.00,
	#Offensive Abilities,
	'suppression' : 0.00,
	'soft_attack' : 3.00,
	'hard_attack' : 1.00,
	'air_attack' : 0.10,
   	'amphibious' :    { 'attack' : -0.70 },
	'fort' : {'attack' : 0.10},
	'urban' : {
		'attack' : -0.15,
		'defence' : -0.3
	},
   	'marsh' : 	{
        'attack' : -0.15,
		'movement' : -0.70,
		'defence' : -0.25
    },
	'jungle' : 	{
        'attack' : -0.15,
		'movement' : -0.30,
		'defence' : -0.25
	},
	'forest' : 	{
        'attack' : -0.15,
		'movement' : -0.30,
		'defence' : -0.15
	},
	'woods' : 	{
        'attack' : -0.05 ,
		'movement' : -0.05,
		'defence' : -0.10
    },
	'mountain' : 	{
        'attack' : -0.15,
		'movement' : -0.50,
		'defence' : -0.30
    },
	'hills' : 	{
        'movement' : -0.05,
		'attack' : -0.10
    },
	'combat_width' : 0,
	'completion_size' : 0.8,
	'on_completion' : 'artillery_practical',
	'priority' : 4
},

'strategic_bomber' : {
	'type' : 'air',
	'active' : 'no',
	'sprite' : 'Bomber',
	'is_bomber' : 'yes',
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' : 17.00,
	'build_cost_manpower' : 2.00,
	'build_time' : 300,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 200.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 3.00,
	'fuel_consumption' : 4.60,
	'range' : 800.00,
	'radio_strength' : 1,
	#Detection Abilities,
	'surface_detection' : 2.00,
	'air_detection' : 2.00,
	#Defensive Abilities,
	'surface_defence' : 8.00,
	'air_defence' : 6.00,
	#Offensive Abilities,
	'soft_attack' : 1.00,
	'hard_attack' : 0.00,
	'sea_attack' : 0.00,
	'air_attack' : 2.00,
	'strategic_attack' : 7.00,
	'sub_attack' : 0.00,
	'completion_size' : 5.1,
	'on_completion' : 'four_engine_aircraft_practical',
	'priority' : 4
},

'submarine' : {
	'type' : 'naval',
	'sprite' : 'Submarine',
	'is_sub' : 'yes',
	'active' : 'no',
	'hull' : 0.5,
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 25,
	'default_morale' : 0.35,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' :  4.00,
	'build_cost_manpower' : 0.4,
	'build_time' : 240,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 10.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 0.30,
	'fuel_consumption' : 0.58,
	'range' : 800.00,
	'distance' : 0.15,
	'radio_strength' : 1,
	#Detection Abilities,
	'surface_detection' : 2.00,
	'air_detection' : 1.00,
	'sub_detection' : 1.00,
	'visibility' : 5,
	#Defensive Abilities,
	'sea_defence' : 5.00,
	'air_defence' : 1.00,
	#Offensive Abilities,
	'convoy_attack' : 1.00,
	'sea_attack' : 2.00,
	'sub_attack' : 0.00,
	'air_attack' : 1.00,
	'shore_bombardment' : 0.00,
	'completion_size' : 1,
	'on_completion' : 'submarine_practical',
	'priority' : 3
},

'super_heavy_armor_brigade' : {
	'type' : 'land',
	'sprite' : 'Tank',
	'active' : 'no',
	'unit_group' : 'armor_unit_type',
	#Size Definitions,
	'max_strength' : 10,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 22,
	'build_cost_manpower' : 0.67,
	'build_time' : 210,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 3.00,
	'transport_weight' : 25.00,
	'supply_consumption' : 2.5,
	'fuel_consumption' : 5.5,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 8.00,	# was 11.00,
	'toughness' : 5.00,
	'softness' : 0.05,
	'air_defence' : 0.40,
	'armor_value' : 13,
	#Offensive Abilities,
	'suppression' : 0.00,
	'soft_attack' : 4.00,
	'hard_attack' : 7.00,
	'air_attack' : 0.10,
	'ap_attack' : 13,
   	'amphibious' :    { 'attack' : -3 },
    'river' :         { 'attack' : -0.50 },
	'urban' : {
		'attack' : -0.60,
		'defence' : -0.85
	},
   	'marsh' : 	{
        'attack' : -0.50 ,
		'movement' : -0.95,
		'defence' : -0.75
	},
	'jungle' : {
        'attack' : -0.70,
		'movement' : -0.60,
		'defence' : -0.75
	},
	'forest' : {
        'attack' : -0.50,
		'movement' : -0.60,
		'defence' : -0.6
	},
	'woods' : {
        'attack' : -0.20 ,
		'movement' : -0.30,
		'defence' : -0.50
    },
	'mountain' : 	{
        'attack' : -0.55,
		'movement' : -0.60,
		'defence' : -0.85
	},
	'hills' : {
        'attack' : -0.30 ,
		'movement' : -0.30,
		'defence' : -0.50
    },
	'combat_width' : 0,
	'completion_size' : 3,
	'on_completion' : 'armour_practical',
	'priority' : 10,
},

'super_heavy_battleship' : {
	'type' : 'naval',
	'sprite' : 'battleship',
	'capital' : 'yes',
	'can_be_pride' : 'yes',
	'hull' : 2.5,
	'active' : 'no',
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' : 10.50,
	'build_cost_manpower' : 3.00,
	'build_time' : 760,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 18.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 1.00,
	'fuel_consumption' : 2.30,
	'range' : 4000.00,
	'distance' : 0.38,
	'radio_strength' : 1.25,
	#Detection Abilities,
	'surface_detection' : 1.00,
	'air_detection' : 1.00,
	'sub_detection' : 1.00,
	'visibility' : 95.00,
	#Defensive Abilities,
	'sea_defence' : 2.00,
	'air_defence' : 4.00,
	#Offensive Abilities,
	'convoy_attack' : 22.00,
	'sea_attack' : 22.00,
	'sub_attack' : 0.00,
	'air_attack' : 1.00,
	'shore_bombardment' : 14.00,
	'completion_size' : 8,
	'on_completion' : 'capitalship_practical',
	'priority' : 9
},

'tactical_bomber' : {
	'type' : 'air',
	'active' : 'no',
	'sprite' : 'Tactical',
	'is_bomber' : 'yes',
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' : 16.00,
	'build_cost_manpower' : 1.60,
	'build_time' : 220,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 200.00,
	'transport_capability' : 0.00,
	'supply_consumption' : 2.00,
	'fuel_consumption' : 3.45,
	'range' : 400.00,
	'radio_strength' : 1,
	#Detection Abilities,
	'surface_detection' : 2.00,
	'air_detection' : 0.00,
	#Defensive Abilities,
	'surface_defence' : 3.50,
	'air_defence' : 4.25,
	#Offensive Abilities,
	'soft_attack' : 6.00,
	'hard_attack' : 2.50,
	'sea_attack' : 2.75,
	'air_attack' : 1.25,
	'strategic_attack' : 1.50,
	'sub_attack' : 1.50,
	'completion_size' : 3.5,
	'on_completion' : 'twin_engine_aircraft_practical',
	'priority' : 3,
},

'tank_destroyer_brigade' : {
	'type' : 'land',
	'sprite' : 'Tank',
	'active' : 'no',
	'unit_group' : 'direct_fire_unit_type',
	'is_mobile' : 'yes',
	#Size Definitions,
	'max_strength' : 10,
	'default_organisation' : 30,
	'default_morale' : 0.30,
	'officers' : 100,
	#Building Costs,
	'build_cost_ic' : 7.00,
	'build_cost_manpower' : 1.67,
	'build_time' : 180,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 6.00,
	'transport_weight' : 8.00,
	'supply_consumption' : 1.0,
	'fuel_consumption' : 2,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 3.00,	# was 4.33,
	'toughness' : 2.00,
	'softness' : 0.20, #30,
	'air_defence' : 0.80,
	'armor_value' : 3,
	#Offensive Abilities,
	'suppression' : 0.00,
	'soft_attack' : 2.00,
	'hard_attack' : 2.00,
	'air_attack' : 0.10,
	'ap_attack' : 5,
	'amphibious' :    { 'attack' : -0.70  	 },
   	'river' :         { 'attack' : -0.1 },
	'urban' : {
        'attack' : -0.50,
		'defence' : -0.40
    },
    'marsh' : {
        'attack' : -0.40 ,
        'movement' : -0.80,
        'defence' : -0.60
    },
	'jungle' : {
        'attack' : -0.40,
        'movement' : -0.40,
        'defence' : -0.35
    },
	'forest' : 	{ 
        'attack' : -0.30,
		'movement' : -0.40,
		'defence' : -0.30
    },
	'woods' : 	{
        'attack' : -0.20 ,
		'movement' : -0.10,
		'defence' : -0.1
    },
	'mountain' : 	{
        'attack' : -0.40,
		'movement' : -0.40,
		'defence' : -0.30
    },
	'hills' : {
        'attack' : -0.10,
		'movement' : -0.10,
		'defence' : -0.15
    },
	'combat_width' : 0,
	'completion_size' : 1,
	'on_completion' : 'armour_practical',
	'priority' : 4,
},

'transport_ship' : {
	'type' : 'naval',
	'sprite' : 'Transportship',
	'radio_strength' : 1,
	'transport' : 'yes',
	'hull' : 1,
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 20,
	'default_morale' : 0.20,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' : 5.00,
	'build_cost_manpower' : 1.00,
	'build_time' : 90,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 15.00,
	'transport_capability' : 40.00,
	'supply_consumption' : 0.30,
	'fuel_consumption' : 1.15,
	'range' : 3000.00,
	'distance' : 0.10,
	#Detection Abilities,
	'surface_detection' : 0.00,
	'air_detection' : 0.00,
	'sub_detection' : 0.00,
	'visibility' : 90.00,
	#Defensive Abilities,
	'sea_defence' : 0.00,
	'air_defence' : 0.00,
	#Offensive Abilities,
	'convoy_attack' : 0.00,
	'sea_attack' : 0.00,
	'sub_attack' : 0.00,
	'air_attack' : 0.00,
	'shore_bombardment' : 0.00,
	'completion_size' : 0.5,
	'on_completion' : 'transport_practical',
	'priority' : 1,
},

'transport_plane' : {
	'type' : 'air',
	'active' : 'no',
	'sprite' : 'Transport',
	'is_bomber' : 'yes',
	#Size Definitions,
	'max_strength' : 100,
	'default_organisation' : 20,
	'default_morale' : 0.30,
	'officers' : 0,
	#Building Costs,
	'build_cost_ic' : 20.00,
	'build_cost_manpower' : 4.00,
	'build_time' : 150,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 200.00,
	'transport_capability' : 15.00,
	'supply_consumption' : 1.00,
	'fuel_consumption' : 2.30,
	'range' : 500.00,
	'radio_strength' : 1,
	#Detection Abilities,
	'surface_detection' : 0.00,
	'air_detection' : 0.00,
	#Defensive Abilities,
	'surface_defence' : 0.00,
	'air_defence' : 1.00,
	#Offensive Abilities,
	'soft_attack' : 0.00,
	'hard_attack' : 0.00,
	'sea_attack' : 0.00,
	'air_attack' : 0.00,
	'strategic_attack' : 0.00,
	'sub_attack' : 0.00,
	'completion_size' : 3.0,
	'on_completion' : 'four_engine_aircraft_practical',
	'priority' : 1
},

'waffen_brigade' : {
	'usable_by' : 'GER' , #Only nations listed here can build this unit or see tooltips on techs relating to it.,
	'type' : 'land',
	'sprite' : 'Elite',
	'is_mobile' : 'yes',
	'unit_group' : 'infantry_unit_type', # Combined arms 'type'. Other 'type's are 'support_unit_type', 'direct_fire_unit_type', 'armor_unit_type', and 'artillery_unit_type'.,
	#lim'ITA'tions,
	'minimum_of_type' : 6, #You can always build this many,
	'max_percentage_of_type' : 0.04, #Percentage of all 'Infantry'. Special forces don't count as Inf,
	'available_trigger' : {
		# This trigger must be true to build these units. 'no't currently used. Modders go for it though.,
	},
	#Size Definitions,
	'max_strength' : 30,
	'default_organisation' : 50,
	'default_morale' : 0.50,
	'officers' : 130,
	#Building Costs,
	'build_cost_ic' : 4.67,
	'build_cost_manpower' : 3.33,
	'build_time' : 120,
	'repair_cost_multiplier' : 0.05,
	#Misc Abilities,
	'maximum_speed' : 8.00, #'no'w are MOT, we sold out.,
	'transport_weight' : 10.00,
	'supply_consumption' : 1.00,
	'fuel_consumption' : 1.00,
	'radio_strength' : 1,
	#Defensive Abilities,
	'defensiveness' : 4.00,
	'toughness' : 3.00,
	'softness' : 0.70,
	'air_defence' : 2.00,
	#Offensive Abilities,
	'suppression' : 3.00,
	'soft_attack' : 2.00,
	'hard_attack' : 0.67,
	'air_attack' : 0.33,
	'ap_attack' : 1,
	'urban' : {
        'attack' : 0.20,
        'defence' : 0.15
    },
	'amphibious' : { 'attack' : -0.20 },
	'marsh' : {
		'attack' : -0.10,
		'movement' : -0.80
	},
	'jungle' : {
		'attack' : -0.20,
		'movement' : -0.40
	},
	'forest' : {
		'attack' : -0.10,
		'movement' : -0.30
	},
	'woods' : {
		'attack' : -0.05,
		'movement' : -0.20
	},
	'mountain' : {
		'attack' : -0.20,
		'movement' : -0.40
	},
	'combat_width' : 1,
	'completion_size' : 0.6,
	'on_completion' : 'mobile_practical',
	'priority' : 9
}
}

aircraft_tech_df = pd.DataFrame(aircraft_tech)
aircraft_doctrine_df = pd.DataFrame(aircraft_doctrine)
armor_tech_df = pd.DataFrame(armor_tech)
arty_tech_df = pd.DataFrame(arty_tech)
buildings_df = pd.DataFrame(buildings)
combined_arms_df = pd.DataFrame(combined_arms)
defines_df = pd.DataFrame(defines)
country_df = defines_df[['country']]
economy_df = pd.DataFrame[['economy']]
military_df = pd.DataFrame[['military']]
goods_cost_df = pd.DataFrame([goods_cost])
industry_tech_df = pd.DataFrame(industry_tech)
infantry_tech_df = pd.DataFrame(infantry_tech)
land_doctrine_df = pd.DataFrame(land_doctrine)
laws_df = pd.DataFrame(laws)
ministers_df = pd.DataFrame(minsiters)
static_modifiers_df = pd.DataFrame(static_modifiers)
base_values_df = static_modifiers_df[['base_values']]
naval_doctrine_df = pd.DataFrame(naval_doctrine)
naval_tech_df = pd.DataFrame(naval_tech)
stratgic_resources_df = pd.DataFrame(stratgic_resources)
secret_tech_df = pd.DataFrame(secret_tech)
categories_df = pd.DataFrame(categories)
theories_df = pd.DataFrame(theories)
units_df = pd.DataFrame(units)


def inputs():
    y = {'base_ic' : raw_input('Base IC '),
    'SUPPLIES': raw_input('Amount of Supplies '),
    'FUEL': raw_input('Amount of Fuel '),
    'MONEY' : raw_input('Amount of Money '),
    'CRUDE_OIL': raw_input('Amount of Crude Oil '),
    'METAL' : raw_input('Amount of Metal '),
    'ENERGY' : raw_input('Amount of Energy '),
    'RARE_MATERIALS': raw_input('Amount of Rare Materials '),
    'FUEL__PRO': raw_input('Amount of Fuel Produced'),
    'MONEY_PRO' : raw_input('Amount of Money Produced'),
    'CRUDE_OIL_PRO': raw_input('Amount of Crude Oil Produced'),
    'METAL_PRO' : raw_input('Amount of Metal Produced'),
    'ENERGY_PRO' : raw_input('Amount of Energy Produced'),
    'RARE_MATERIALS_PRO': raw_input('Amount of Rare Materials Produced'),
    'head_of_state' : raw_input('head_of_state '),
	'head_of_government' : raw_input('head_of_government '),
	'foreign_minister' : raw_input('foreign_minister '),
	'armament_minister' : raw_input('armament_minister '),
	'minister_of_security' : raw_input('minister_of_security '),
	'minister_of_intelligence' : raw_input('minister_of_intelligence '),
	'chief_of_staff' : raw_input('chief_of_staff '),
	'chief_of_army' : raw_input('chief_of_army '),
	'chief_of_navy' : raw_input('chief_of_navy '),
	'chief_of_air' : raw_input('chief_of_air '),
	 
	'infantry_theory' : raw_input('infantry_theory '),
	'infantry_practical' : raw_input('infantry_practical '),
	'militia_theory' : raw_input('militia_theory '),
	'militia_practical' : raw_input('militia_practical '),
	'mobile_theory' : raw_input('mobile_theory '),
	'mobile_practical' : raw_input('mobile_practical '),
	'artillery_theory' : raw_input('artillery_theory '),
	'artillery_practical' : raw_input('artillery_practical '),
	'rocket_science' : raw_input('rocket_science '),
	'rocket_practical' : raw_input('rocket_practical '),
	'naval_engineering' : raw_input('naval_engineering '),
	'destroyer_practical' : raw_input('destroyer_practical '),
	'cruiser_practical' : raw_input('cruiser_practical '),
	'capitalship_practical' : raw_input('capitalship_practical '),
	'carrier_practical' : raw_input('carrier_practical '),
	'submarine_engineering' : raw_input('submarine_engineering'  ),
	'submarine_practical' : raw_input('submarine_practical '),
	'electornicegineering_theory' : raw_input('electornicegineering_theory'  ),
	'electornicegineering_practical' : raw_input('electornicegineering_practical '),
	'automotive_theory' : raw_input('automotive_theory'  ),
	'armour_practical' : raw_input('armour_practical '),
	'aeronautic_engineering' : raw_input('aeronautic_engineering'  ),
	'single_engine_aircraft_practical' : raw_input('single_engine_aircraft_practical'  ),
	'twin_engine_aircraft_practical' : raw_input('twin_engine_aircraft_practical '),
	'four_engine_aircraft_practical' : raw_input('four_engine_aircraft_practical '),
	'spearhead_theory' : raw_input('spearhead_theory'  ),
	'superior_firepower_theory' : raw_input('superior_firepower_theory'  ),
	'grand_battleplan_theory' : raw_input('grand_battleplan_theory'  ),
	'human_wave_theory' : raw_input('human_wave_theory'  ),
	'land_doctrine_practical': raw_input('land_doctrine_practical '),
	'naval_doctrine_practical' : raw_input('naval_doctrine_practical'  ),
	'base_strike_doctrine' : raw_input('base_strike_doctrine '),
	'fleet_in_being_doctrine' : raw_input('fleet_in_being_doctrine'  ),
	'air_doctrine_practical' : raw_input('air_doctrine_practical'  ),
	'fighter_focus' : raw_input('fighter_focus '),
	'cas_focus' : raw_input('cas_focus '),
	'tac_focus' : raw_input('tac_focus '),
	'nav_focus' : raw_input('nav_focus '),
	'strategic_air_focus' : raw_input('strategic_air_focus'  ),
	'sealane_interdiction_doctrine' : raw_input('sealane_interdiction_doctrine '),
	'mechanicalengineering_theory' : raw_input('mechanicalengineering_theory '),
	'chemical_engineering' : raw_input('chemical_engineering'  ),
	'jetengine_theory' : raw_input('jetengine_theory'  ),
	'jetengine_practical' : raw_input('jetengine_practical'  ),
	'nuclear_physics' : raw_input('nuclear_physics '),
	'nuclear_bomb' : raw_input('nuclear_bomb'  ),
	'transport_practical' : raw_input('transport_practical '),
	'officers_ratio' : raw_input('officers_ratio '),
	'construction_practical' : raw_input('construction_practical '),
	 
	'combat_medicine' : raw_input('combat_medicine '),
	'first_aid' : raw_input('first_aid '),
	'agriculture' : raw_input('agriculture '),
	'industral_production' : raw_input('industral_production '),
	'industral_efficiency' : raw_input('industral_efficiency '),
	'oil_to_coal_conversion' : raw_input('oil_to_coal_conversion '),
	'supply_production' : raw_input('supply_production '),
	'electronic_mechanical_egineering' : raw_input('electronic_mechanical_egineering '),
	'radio_technology' : raw_input('radio_technology '),
	'radio_detection_equipment' : raw_input('radio_detection_equipment '),
	'radio' : raw_input('radio '),
	'census_tabulation_machine' : raw_input('census_tabulation_machine '),
	'mechnical_computing_machine' : raw_input('mechnical_computing_machine '),
	'encryption_machine' : raw_input('encryption_machine '),
	'construction_engineering' : raw_input('construction_engineering '),
	'advanced_construction_engineering' : raw_input('advanced_construction_engineering '),
	'rocket_tests' : raw_input('rocket_tests '),
	'atomic_research' : raw_input('atomic_research '),
	'oil_refinning' : raw_input('oil_refinning '),
	'steel_production' : raw_input('steel_production '),
	'raremetal_refinning_techniques' : raw_input('raremetal_refinning_techniques '),
	'coal_processing_technologies' : raw_input('coal_processing_technologies '),
	'education' : raw_input('education '),
	'supply_transportation' : raw_input('supply_transportation '),
	'supply_organisation' : raw_input('supply_organisation '),
	'civil_defence' : raw_input('civil_defence '),
	'heavy_aa_guns' : raw_input('heavy_aa_guns '),
	'militia_smallarms' : raw_input('militia_smallarms '),
	'militia_support' : raw_input('militia_support '),
	'militia_guns' : raw_input('militia_guns '),
	'militia_at' : raw_input('militia_at '),
	'infantry_activation' : raw_input('infantry_activation '),
	'smallarms_technology' : raw_input('smallarms_technology '),
	'infantry_support' : raw_input('infantry_support '),
	'infantry_guns' : raw_input('infantry_guns '),
	'infantry_at' : raw_input('infantry_at '),
	'schwerpunkt' : raw_input('schwerpunkt '),
	'blitzkrieg' : raw_input('blitzkrieg '),
	'mobile_warfare' : raw_input('mobile_warfare '),
	'elastic_defence' : raw_input('elastic_defence '),
	'operational_level_command_structure' : raw_input('operational_level_command_structure '),
	'tactical_command_structure' : raw_input('tactical_command_structure '),
	'mechanized_offensive' : raw_input('mechanized_offensive '),
	'delay_doctrine' : raw_input('delay_doctrine '),
	'integrated_support_doctrine' : raw_input('integrated_support_doctrine '),
	'infantry_warfare' : raw_input('infantry_warfare '),
	'special_forces' : raw_input('special_forces '),
	'assault_concentration' : raw_input('assault_concentration '),
	'central_planning' : raw_input('central_planning '),
	'mass_assault' : raw_input('mass_assault '),
	'peoples_army' : raw_input('peoples_army '),
	'large_front' : raw_input('large_front '),
	'guerilla_warfare' : raw_input('guerilla_warfare '),
	'operational_level_organisation' : raw_input('operational_level_organisation '),
	'cavalry_smallarms' : raw_input('cavalry_smallarms '),
	'cavalry_support' : raw_input('cavalry_support '),
	'cavalry_guns' : raw_input('cavalry_guns '),
	'cavalry_at' : raw_input('cavalry_at '),
	'mortorised_infantry' : raw_input('mortorised_infantry '),
	'art_barrell_ammo' : raw_input('art_barrell_ammo '),
	'art_carriage_sights' : raw_input('art_carriage_sights '),
	'lighttank_brigade' : raw_input('lighttank_brigade '),
	'lighttank_armour' : raw_input('lighttank_armour '),
	'single_engine_aircraft_design' : raw_input('single_engine_aircraft_design '),
	'basic_aeroengine' : raw_input('basic_aeroengine'  ),
	'basic_small_fueltank' : raw_input('basic_small_fueltank'  ),
	'basic_single_engine_airframe' : raw_input('basic_single_engine_airframe'  ),
	'basic_aircraft_machinegun' : raw_input('basic_aircraft_machinegun '),
	'twin_engine_aircraft_design' : raw_input('twin_engine_aircraft_design '),
	'basic_aeroengine' : raw_input('basic_aeroengine '),
	'basic_twin_engine_airframe' : raw_input('basic_twin_engine_airframe '),
	'basic_four_engine_airframe' : raw_input('basic_four_engine_airframe '),
	'battlecruiser_technology' : raw_input('battlecruiser_technology '),
	'battlecruiser_antiaircraft' : raw_input('battlecruiser_antiaircraft '),
	'battlecruiser_engine' : raw_input('battlecruiser_engine '),
	'battlecruiser_armour' : raw_input('battlecruiser_armour '),
	'battleship_technology' : raw_input('battleship_technology '),
	'capitalship_armament' : raw_input('capitalship_armament '),
	'battleship_antiaircraft' : raw_input('battleship_antiaircraft '),
	'battleship_engine' : raw_input('battleship_engine '),
	'battleship_armour' : raw_input('battleship_armour '),
	'destroyer_technology' : raw_input('destroyer_technology '),
	'destroyer_armament' : raw_input('destroyer_armament '),
	'destroyer_engine' : raw_input('destroyer_engine '),
	'destroyer_armour' : raw_input('destroyer_armour '),
	'heavycruiser_technology' : raw_input('heavycruiser_technology '),
	'heavycruiser_armament' : raw_input('heavycruiser_armament '),
	'heavycruiser_antiaircraft' : raw_input('heavycruiser_antiaircraft '),
	'heavycruiser_engine' : raw_input('heavycruiser_engine '),
	'heavycruiser_armour' : raw_input('heavycruiser_armour '),
	'lightcruiser_technology' : raw_input('lightcruiser_technology '),
	'lightcruiser_armament' : raw_input('lightcruiser_armament '),
	'lightcruiser_antiaircraft' : raw_input('lightcruiser_antiaircraft '),
	'lightcruiser_engine' : raw_input('lightcruiser_engine '),
	'lightcruiser_armour' : raw_input('lightcruiser_armour '),
	'smallwarship_radar' : raw_input('smallwarship_radar '),
	'submarine_technology' : raw_input('submarine_technology '),
	'submarine_engine' : raw_input('submarine_engine '),
	'submarine_sonar' : raw_input('submarine_sonar '),
	'submarine_hull' : raw_input('submarine_hull '),
	'sea_lane_defence' : raw_input('sea_lane_defence '),
	'fire_control_system_training' : raw_input('fire_control_system_training '),
	'fleet_auxiliary_submarine_doctrine' : raw_input('fleet_auxiliary_submarine_doctrine '),
	'trade_interdiction_submarine_doctrine' : raw_input('trade_interdiction_submarine_doctrine '),
	'cruiser_warfare' : raw_input('cruiser_warfare '),
	'submarine_crew_training' : raw_input('submarine_crew_training '),
	'fighter_pilot_training' : raw_input('fighter_pilot_training '),
	'fighter_groundcrew_training' : raw_input('fighter_groundcrew_training '),
	'interception_tactics' : raw_input('interception_tactics '),
	'fighter_ground_control' : raw_input('fighter_ground_control '),
	'bomber_targerting_focus' : raw_input('bomber_targerting_focus '),
	'fighter_targerting_focus' : raw_input('fighter_targerting_focus '),
	'cas_pilot_training' : raw_input('cas_pilot_training '),
	'cas_groundcrew_training' : raw_input('cas_groundcrew_training '),
	'ground_attack_tactics' : raw_input('ground_attack_tactics '),
	'forward_air_control' : raw_input('forward_air_control '),
	'battlefield_interdiction' : raw_input('battlefield_interdiction '),
	'tac_pilot_training' : raw_input('tac_pilot_training '),
	'tac_groundcrew_training' : raw_input('tac_groundcrew_training '),
	'interdiction_tactics' : raw_input('interdiction_tactics '),
	'logistical_strike_tactics' : raw_input('logistical_strike_tactics '),
	'installation_strike_tactics' : raw_input('installation_strike_tactics '),
	'airbase_strike_tactics' : raw_input('airbase_strike_tactics '),
	'tactical_air_command' : raw_input('tactical_air_command '),
	'nav_pilot_training' : raw_input('nav_pilot_training '),
	'nav_groundcrew_training' : raw_input('nav_groundcrew_training '),
	'portstrike_tactics' : raw_input('portstrike_tactics '),
	'navalstrike_tactics' : raw_input('navalstrike_tactics '),
	'naval_air_targeting' : raw_input('naval_air_targeting '),
	'naval_tactics' : raw_input('naval_tactics '),
	'heavy_bomber_pilot_training' : raw_input('heavy_bomber_pilot_training '),
	'heavy_bomber_groundcrew_training' : raw_input('heavy_bomber_groundcrew_training'  ),
	'strategic_bombardment_tactics' : raw_input('strategic_bombardment_tactics '),
	'airborne_assault_tactics' : raw_input('airborne_assault_tactics '),
	'strategic_air_command' : raw_input('strategic_air_command '),
	'cas_development' : raw_input('cas_development '),
	'nav_development' : raw_input('nav_development '),
	'basic_strategic_bomber' : raw_input('basic_strategic_bomber '),

	'training_laws' : raw_input('advanced_training '),
	'press_laws' : raw_input('propaganda_press '),
	'industrial_policy_laws' : raw_input('mixed_industry '),
	'education_investment_law' : raw_input('medium_large_education_investment '),
	'economic_law' : raw_input('basic_mobilisation '),
	'conscription_law' : raw_input('volunteer_army '),
	'civil_law' : raw_input('totalitarian_system '),
    }
    return y

goods_cost_dft = goods_cost_df.T
goods_cost_dft = goods_cost_dft.rename(columns={0 : 'Cost'})
inputs = pd.DataFrame(test).T

economic_effects = pd.merge(ministers_df.T, inputs, left_index=True, right_on=0, how='inner')
laws_dict ={
test.get('training_laws') : laws.get('training_laws').get(test.get('training_laws')),
test.get('press_laws') : laws.get('press_laws').get(test.get('press_laws')),
test.get('industrial_policy_laws') : laws.get('industrial_policy_laws').get(test.get('industrial_policy_laws')),
test.get('education_investment_law') : laws.get('education_investment_law').get(test.get('education_investment_law')),
test.get('economic_law') : laws.get('economic_law').get(test.get('economic_law')),
test.get('conscription_law') : laws.get('conscription_law').get(test.get('conscription_law')),
test.get('civil_law') : laws.get('civil_law').get(test.get('civil_law'))
}
laws_df = pd.DataFrame(laws_dict).T
economic_effects = economic_effects.append(laws_df)
base_values_df = static_modifiers_df[['base_values']]
economic_effects = economic_effects.append(base_values_df.T)

economic_effects.dropna(axis=1, how='all')


    actual_ic = y.get(int('base_ic'))
	goods_cost_df = pd.DataFrame([goods_cost])
	gods_cost_dft = goods_cost_df.T
	x = goods_cost_dft.rename(columns={0 : 'Cost'})
	x['Used'] = pd.Series({'METAL' : -1,'ENERGY': -2,'RARE_MATERIALS' : -.5 }) * actual_ic
	x['Converted From'] = pd.Series({'CRUDE_OIL' : -.5, 'ENERGY' : -.05/.1}) * actual_ic
	x['Converted To'] = pd.Series({'CRUDE_OIL' : .05, 'FUEL' : .5}) * actual_ic
	z = x.pop('base_ic')
	x['Stockpile'] = pd.Series(z)
	x['Produced'] = pd.Series()
	x['Traded'] = pd.Series()
	x.set_value('MONEY','Cost',1)
	return x.T




base_ic = 100
actual_ic = base_ic
goods_cost_dft = goods_cost_df.T
x = goods_cost_dft.rename(columns={0 : 'Cost'})
x['Used'] = pd.Series({'METAL' : -1,'ENERGY': -2,'RARE_MATERIALS' : -.5 }) * actual_ic
x['Converted From'] = pd.Series({'CRUDE_OIL' : -.5, 'ENERGY' : -.05/.1}) * actual_ic
x['Converted To'] = pd.Series({'CRUDE_OIL' : .05, 'FUEL' : .5}) * actual_ic
x['Stockpile'] = pd.Series(inputs())
x['Produced'] = pd.Series()
x['Traded'] = pd.Series()
x.set_value('MONEY','Cost',1)
x.T

x = units_df.T
x[[u'active', u'air_attack', u'air_defence', u'air_detection',
       u'ap_attack', u'armor_value',
       u'build_cost_ic', u'build_cost_manpower',
       u'build_time', u'capital',
       u'combat_width', u'completion_size', u'convoy_attack',
       u'default_morale', u'default_organisation', u'defensiveness', 
       u'distance', 
       u'fuel_consumption', u'hard_attack', u'hull', 
       u'is_buildable', u'max_percentage_of_type',
       u'max_strength', u'maximum_speed', u'minimum_of_type', u'officers', u'on_completion', u'priority',
       u'radio_strength', u'range', u'repair_cost_multiplier',
       u'sea_attack', u'sea_defence', u'shore_bombardment', u'soft_attack',
       u'softness', u'strategic_attack', u'sub_attack',
       u'sub_detection', u'supply_consumption', u'suppression',
       u'surface_defence', u'surface_detection', u'toughness', u'transport',
       u'transport_capability', u'transport_weight', u'type', u'unit_group',
       u'usable_by', u'visibility']]


'global_crude_oil', 'global_ic', 'global_money', 'global_resources', 'global_supplies','industrial_efficiency', 'peace_consumer_goods_demand', 'supply_consumption', 'supply_throughput', 'war_consumer_goods_demand', 0