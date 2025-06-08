const {
    Client,
    GatewayIntentBits,
    SlashCommandBuilder,
    EmbedBuilder,
    PermissionFlagsBits,
} = require("discord.js");
const fs = require("fs").promises;
const path = require("path");

// Keep alive server
require("./keep_alive");

// Bot configuration
const client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildMessages,
        GatewayIntentBits.MessageContent,
    ],
});

// Data storage
let botData = {
    companies: {},
    groups: {},
    users: {},
    charts: {
        MelOn: {},
        Genies: {},
        Bugs: {},
        FLO: {},
    },
    previousCharts: {
        MelOn: {},
        Genies: {},
        Bugs: {},
        FLO: {},
    },
    mentions: {},
    lastMentionReset: 0,
};

// Constants
const COMPANY_SIZES = {
    small: { baseFunds: 50000, debutPopularity: [50, 500], maxFunds: 200000 },
    medium: { baseFunds: 150000, debutPopularity: [800, 3000], maxFunds: 500000 },
    big: { baseFunds: 500000, debutPopularity: [2500, 8000], maxFunds: 2000000 },
};

const MUSIC_SHOWS = [
    "Music Bank",
    "Show!Music Core",
    "M COUNTDOWN",
    "Inkigayo",
    "The Show",
];
const CHART_PLATFORMS = ["MelOn", "Genies", "Bugs", "FLO"];

// Utility functions
function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function calculateGroupTier(popularity, fans) {
    if (popularity >= 50000 && fans >= 200000) return "icon";
    if (popularity >= 20000 && fans >= 80000) return "big";
    if (popularity >= 5000 && fans >= 20000) return "medium";
    if (popularity >= 1000 && fans >= 3000) return "small";
    return "nugu";
}

function generateSales(groupTier, popularity) {
    const baseSales = {
        nugu: getRandomInt(10, 100),
        small: getRandomInt(100, 1000),
        medium: getRandomInt(1000, 10000),
        big: getRandomInt(10000, 100000),
        icon: getRandomInt(50000, 500000),
    };

    const popularityMultiplier = Math.max(1, popularity / 1000);
    return Math.floor(baseSales[groupTier] * popularityMultiplier);
}

function updateCharts() {
    // Save current charts as previous
    botData.previousCharts = JSON.parse(JSON.stringify(botData.charts));
    
    // Don't completely clear charts - songs should persist and move gradually
    // Only remove songs that have been charting for too long or dropped too low
    Object.keys(botData.charts).forEach((platform) => {
        Object.entries(botData.charts[platform]).forEach(([song, position]) => {
            // 10% chance to drop off if position is > 50, 5% chance if position is > 80
            const dropChance = position > 80 ? 0.05 : position > 50 ? 0.1 : 0.02;
            if (Math.random() < dropChance) {
                delete botData.charts[platform][song];
            }
        });
    });

    // Get all groups with albums
    const groupsWithAlbums = Object.values(botData.groups).filter(
        (group) => group.albums.length > 0,
    );

    groupsWithAlbums.forEach((group) => {
        group.albums.forEach((album) => {
            const groupTier = calculateGroupTier(group.popularity, group.fans);
            const chartingData = {
                nugu: { chance: 0.0, positions: [90, 100] }, // Don't chart
                small: { chance: 0.1, positions: [70, 100] }, // Chart very lowly
                medium: { chance: 0.5, positions: [30, 80] },
                big: { chance: 0.8, positions: [5, 50] },
                icon: { chance: 0.95, positions: [1, 20] },
            }[groupTier];

            const key = `${group.name} - ${album.name}`;

            CHART_PLATFORMS.forEach((platform) => {
                const currentPosition = botData.charts[platform][key];
                
                if (currentPosition) {
                    // Song is already charting - move it slightly
                    const movement = getRandomInt(-5, 5);
                    let newPosition = currentPosition + movement;
                    newPosition = Math.max(1, Math.min(100, newPosition));
                    
                    // Ensure no duplicate positions
                    while (Object.values(botData.charts[platform]).includes(newPosition) && newPosition !== currentPosition) {
                        newPosition = currentPosition + getRandomInt(-3, 3);
                        newPosition = Math.max(1, Math.min(100, newPosition));
                    }
                    
                    botData.charts[platform][key] = newPosition;
                } else if (Math.random() < chartingData.chance) {
                    // New entry to charts
                    let position = getRandomInt(chartingData.positions[0], chartingData.positions[1]);
                    
                    // If group has #1 on another platform, higher chance for top 10
                    const hasNumber1 = CHART_PLATFORMS.some(p => 
                        Object.entries(botData.charts[p]).some(([s, pos]) => 
                            s.startsWith(group.name) && pos === 1
                        )
                    );
                    
                    if (hasNumber1 && groupTier !== 'nugu' && groupTier !== 'small') {
                        // 70% chance to chart in top 10 if they have #1 elsewhere
                        if (Math.random() < 0.7) {
                            position = getRandomInt(1, 10);
                        }
                    }

                    // Ensure no duplicate positions
                    while (Object.values(botData.charts[platform]).includes(position)) {
                        position = getRandomInt(chartingData.positions[0], chartingData.positions[1]);
                        if (position > 100) break; // Safety break
                    }

                    if (position <= 100) {
                        botData.charts[platform][key] = position;
                    }
                }
            });
        });
    });
}

function getPositionIndicator(song, platform) {
    const currentPos = botData.charts[platform][song];
    const previousPos = botData.previousCharts[platform][song];
    
    if (!previousPos) return " 🆕";
    if (currentPos < previousPos) return ` (+${previousPos - currentPos})`;
    if (currentPos > previousPos) return ` (-${currentPos - previousPos})`;
    return " ▬";
}

function checkPAK(groupName) {
    const group = botData.groups[groupName];
    if (!group) return false;

    let hasAllNumber1 = true;
    CHART_PLATFORMS.forEach((platform) => {
        let hasNumber1 = false;
        Object.entries(botData.charts[platform]).forEach(([song, position]) => {
            if (song.startsWith(groupName) && position === 1) {
                hasNumber1 = true;
            }
        });
        if (!hasNumber1) hasAllNumber1 = false;
    });

    return hasAllNumber1;
}

// Data persistence
async function saveData() {
    try {
        await fs.writeFile("botData.json", JSON.stringify(botData, null, 2));
    } catch (error) {
        console.error("Error saving data:", error);
    }
}

async function loadData() {
    try {
        const data = await fs.readFile("botData.json", "utf8");
        botData = JSON.parse(data);
        
        // Initialize previousCharts if it doesn't exist
        if (!botData.previousCharts) {
            botData.previousCharts = {
                MelOn: {},
                Genies: {},
                Bugs: {},
                FLO: {},
            };
        }
        
        // Initialize social media for existing groups if they don't have it
        Object.values(botData.groups).forEach(group => {
            if (!group.socialMedia) {
                group.socialMedia = {
                    Titter: Math.floor(group.followers * 0.8),
                    TikTak: Math.floor(group.followers * 1.2),
                    YouTuboo: Math.floor(group.followers * 0.6),
                    Isutagram: Math.floor(group.followers),
                };
            }
        });

        // Initialize lastStream for existing users
        Object.values(botData.users).forEach(user => {
            if (!user.lastStream) {
                user.lastStream = 0;
            }
        });

        // Add owner field to existing companies (set to null for existing ones)
        Object.values(botData.companies).forEach(company => {
            if (!company.owner) {
                company.owner = null;
            }
        });
    } catch (error) {
        console.log("No existing data file found, starting fresh");
    }
}

// Slash commands
const commands = [
    new SlashCommandBuilder()
        .setName("help")
        .setDescription("List all available commands"),

    new SlashCommandBuilder()
        .setName("addcompany")
        .setDescription("Add a new company")
        .addStringOption((option) =>
            option
                .setName("name")
                .setDescription("Company name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("size")
                .setDescription("Company size")
                .setRequired(true)
                .addChoices(
                    { name: "Small", value: "small" },
                    { name: "Medium", value: "medium" },
                    { name: "Big", value: "big" },
                ),
        ),

    new SlashCommandBuilder()
        .setName("companyfunds")
        .setDescription("Check company funds")
        .addStringOption((option) =>
            option
                .setName("company")
                .setDescription("Company name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("fundcompany")
        .setDescription("Send money to a company")
        .addStringOption((option) =>
            option
                .setName("company")
                .setDescription("Company name")
                .setRequired(true),
        )
        .addIntegerOption((option) =>
            option
                .setName("amount")
                .setDescription("Amount to send")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("work")
        .setDescription("Work to earn money"),

    new SlashCommandBuilder()
        .setName("daily")
        .setDescription("Claim daily bonus"),

    new SlashCommandBuilder()
        .setName("balance")
        .setDescription("Check your balance"),

    new SlashCommandBuilder()
        .setName("debut")
        .setDescription("Debut a new group")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("albumname")
                .setDescription("Debut album name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("company")
                .setDescription("Company name")
                .setRequired(true),
        )
        .addIntegerOption((option) =>
            option
                .setName("investment")
                .setDescription("Investment amount")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("comeback")
        .setDescription("Group comeback")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("albumname")
                .setDescription("Album name")
                .setRequired(true),
        )
        .addIntegerOption((option) =>
            option
                .setName("investment")
                .setDescription("Investment amount")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("setgroup")
        .setDescription("Set existing group stats")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("company")
                .setDescription("Company name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("debutalbum")
                .setDescription("Debut album name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("comebacks")
                .setDescription("Comeback albums (comma separated)")
                .setRequired(false),
        )
        .addStringOption((option) =>
            option
                .setName("tier")
                .setDescription("Group tier")
                .setRequired(false)
                .addChoices(
                    { name: "Nugu", value: "nugu" },
                    { name: "Small", value: "small" },
                    { name: "Medium", value: "medium" },
                    { name: "Big", value: "big" },
                    { name: "Icon", value: "icon" },
                ),
        ),

    new SlashCommandBuilder()
        .setName("groups")
        .setDescription("List all groups"),

    new SlashCommandBuilder()
        .setName("groupstats")
        .setDescription("View detailed group statistics")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("addwin")
        .setDescription("Add a music show win")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("musicshow")
                .setDescription("Music show")
                .setRequired(true)
                .addChoices(
                    ...MUSIC_SHOWS.map((show) => ({ name: show, value: show })),
                ),
        ),

    new SlashCommandBuilder()
        .setName("resetmentions")
        .setDescription("Reset all group mentions (Admin only)")
        .setDefaultMemberPermissions(PermissionFlagsBits.Administrator),

    new SlashCommandBuilder()
        .setName("brandrep")
        .setDescription("View brand reputation ranking"),

    new SlashCommandBuilder()
        .setName("addkeywords")
        .setDescription("Add keywords for group mentions")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("keywords")
                .setDescription("Keywords (comma separated)")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("sales")
        .setDescription("Generate album sales")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("buzz")
        .setDescription("Generate buzz for a group")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("scandal")
        .setDescription("Generate a scandal (gamble)")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("performance")
        .setDescription("Group performance")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("trainvocals")
        .setDescription("Train group vocals")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("traindance")
        .setDescription("Train group dance")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("trainrap")
        .setDescription("Train group rap")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("sponsorship")
        .setDescription("Attempt to land a sponsorship")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("stream")
        .setDescription("Stream a group album")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("albumname")
                .setDescription("Album name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("charts")
        .setDescription("View music charts")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name (optional)")
                .setRequired(false),
        ),

    new SlashCommandBuilder()
        .setName("payola")
        .setDescription("Pay for better chart positions")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        )
        .addIntegerOption((option) =>
            option
                .setName("amount")
                .setDescription("Amount to pay (minimum 100,000)")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("newpost")
        .setDescription("Make a social media post")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("platform")
                .setDescription("Social media platform")
                .setRequired(true)
                .addChoices(
                    { name: "Titter", value: "Titter" },
                    { name: "TikTak", value: "TikTak" },
                    { name: "YouTuboo", value: "YouTuboo" },
                    { name: "Isutagram", value: "Isutagram" },
                ),
        ),

    new SlashCommandBuilder()
        .setName("companies")
        .setDescription("List all companies and their groups"),

    new SlashCommandBuilder()
        .setName("buyfollowers")
        .setDescription("Buy social media followers (expensive)")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("platform")
                .setDescription("Social media platform")
                .setRequired(true)
                .addChoices(
                    { name: "Titter", value: "Titter" },
                    { name: "TikTak", value: "TikTak" },
                    { name: "YouTuboo", value: "YouTuboo" },
                    { name: "Isutagram", value: "Isutagram" },
                ),
        )
        .addIntegerOption((option) =>
            option
                .setName("amount")
                .setDescription("Number of followers to buy (minimum 1,000)")
                .setRequired(true),
        ),
];

// Command handlers
client.on("interactionCreate", async (interaction) => {
    if (!interaction.isChatInputCommand()) return;

    const { commandName, options, user } = interaction;
    const userId = user.id;

    // Initialize user if doesn't exist
    if (!botData.users[userId]) {
        botData.users[userId] = {
            balance: 1000,
            lastWork: 0,
            lastDaily: 0,
            lastStream: 0,
        };
    }

    try {
        switch (commandName) {
            case "help":
                const embed = new EmbedBuilder()
                    .setTitle("K-pop Roleplay Bot Commands")
                    .setColor(0xff69b4)
                    .addFields(
                        {
                            name: "Economy",
                            value: "/work, /daily, /balance, /fundcompany",
                        },
                        {
                            name: "Companies",
                            value: "/addcompany, /companyfunds",
                        },
                        {
                            name: "Groups",
                            value: "/debut, /comeback, /setgroup, /groups, /groupstats",
                        },
                        {
                            name: "Activities",
                            value: "/addwin, /sales, /buzz, /scandal, /performance, /sponsorship",
                        },
                        {
                            name: "Training",
                            value: "/trainvocals, /traindance, /trainrap",
                        },
                        {
                            name: "Charts & Popularity",
                            value: "/charts, /stream, /payola, /brandrep, /buyfollowers",
                        },
                        {
                            name: "Admin",
                            value: "/resetmentions, /addkeywords",
                        },
                    );
                await interaction.reply({ embeds: [embed] });
                break;

            case "addcompany":
                const companyName = options.getString("name");
                const companySize = options.getString("size");

                if (botData.companies[companyName]) {
                    await interaction.reply(
                        `❌ Company "${companyName}" already exists!`,
                    );
                    return;
                }

                botData.companies[companyName] = {
                    size: companySize,
                    funds: COMPANY_SIZES[companySize].baseFunds,
                    groups: [],
                    owner: userId,
                };

                await interaction.reply(
                    `✅ Company "${companyName}" (${companySize}) created with ${COMPANY_SIZES[companySize].baseFunds.toLocaleString()} :MonthlyPeso:`,
                );
                await saveData();
                break;

            case "companyfunds":
                const checkCompany = options.getString("company");
                if (!botData.companies[checkCompany]) {
                    await interaction.reply(
                        `❌ Company "${checkCompany}" not found!`,
                    );
                    return;
                }

                await interaction.reply(
                    `💰 ${checkCompany} has ${botData.companies[checkCompany].funds.toLocaleString()} :MonthlyPeso:`,
                );
                break;

            case "fundcompany":
                const fundCompany = options.getString("company");
                const fundAmount = options.getInteger("amount");

                if (!botData.companies[fundCompany]) {
                    await interaction.reply(
                        `❌ Company "${fundCompany}" not found!`,
                    );
                    return;
                }

                if (botData.users[userId].balance < fundAmount) {
                    await interaction.reply("❌ Insufficient funds!");
                    return;
                }

                botData.users[userId].balance -= fundAmount;
                botData.companies[fundCompany].funds += fundAmount;

                await interaction.reply(
                    `✅ Sent ${fundAmount.toLocaleString()} :MonthlyPeso: to ${fundCompany}`,
                );
                await saveData();
                break;

            case "work":
                const now = Date.now();
                const lastWork = botData.users[userId].lastWork || 0;

                if (now - lastWork < 3600000) {
                    // 1 hour cooldown
                    const remaining = Math.ceil(
                        (3600000 - (now - lastWork)) / 60000,
                    );
                    await interaction.reply(
                        `⏰ You can work again in ${remaining} minutes!`,
                    );
                    return;
                }

                const workEarnings = getRandomInt(100, 500);
                botData.users[userId].balance += workEarnings;
                botData.users[userId].lastWork = now;

                await interaction.reply(
                    `💼 You worked and earned ${workEarnings} :MonthlyPeso:!`,
                );
                await saveData();
                break;

            case "daily":
                const dailyNow = Date.now();
                const lastDaily = botData.users[userId].lastDaily || 0;

                if (dailyNow - lastDaily < 86400000) {
                    // 24 hour cooldown
                    const remaining = Math.ceil(
                        (86400000 - (dailyNow - lastDaily)) / 3600000,
                    );
                    await interaction.reply(
                        `⏰ You can claim daily bonus in ${remaining} hours!`,
                    );
                    return;
                }

                const dailyBonus = 1000;
                botData.users[userId].balance += dailyBonus;
                botData.users[userId].lastDaily = dailyNow;

                await interaction.reply(
                    `🎁 Daily bonus claimed! +${dailyBonus} :MonthlyPeso:`,
                );
                await saveData();
                break;

            case "balance":
                await interaction.reply(
                    `💰 Your balance: ${botData.users[userId].balance.toLocaleString()} :MonthlyPeso:`,
                );
                break;

            case "debut":
                const debutGroup = options.getString("groupname");
                const debutAlbum = options.getString("albumname");
                const debutCompany = options.getString("company");
                const debutInvestment = options.getInteger("investment");

                if (botData.groups[debutGroup]) {
                    await interaction.reply(
                        `❌ Group "${debutGroup}" already exists!`,
                    );
                    return;
                }

                if (!botData.companies[debutCompany]) {
                    await interaction.reply(
                        `❌ Company "${debutCompany}" not found!`,
                    );
                    return;
                }

                // Check if user owns the company
                if (botData.companies[debutCompany].owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${debutCompany}"! Only the company owner can debut groups.`,
                    );
                    return;
                }

                if (botData.companies[debutCompany].funds < debutInvestment) {
                    await interaction.reply(
                        `❌ ${debutCompany} has insufficient funds!`,
                    );
                    return;
                }

                const company = botData.companies[debutCompany];
                const debutRange = COMPANY_SIZES[company.size].debutPopularity;
                const basePopularity = getRandomInt(debutRange[0], debutRange[1]);
                const investmentMultiplier = Math.max(1, debutInvestment / 10000);

                const finalPopularity = Math.floor(basePopularity * investmentMultiplier);
                const finalFans = Math.floor(finalPopularity * 0.5);
                const finalFollowers = Math.floor(finalPopularity * 2);

                botData.groups[debutGroup] = {
                    name: debutGroup,
                    company: debutCompany,
                    popularity: finalPopularity,
                    fans: finalFans,
                    followers: finalFollowers,
                    socialMedia: {
                        Titter: Math.floor(finalFollowers * 0.8),
                        TikTak: Math.floor(finalFollowers * 1.2),
                        YouTuboo: Math.floor(finalFollowers * 0.6),
                        Isutagram: Math.floor(finalFollowers),
                    },
                    albums: [{ name: debutAlbum, investment: debutInvestment }],
                    skills: { rap: 50, vocal: 50, dance: 50 },
                    wins: 0,
                    paks: 0,
                    keywords: [debutGroup.toLowerCase()],
                };

                company.funds -= debutInvestment;
                company.groups.push(debutGroup);

                const debutTier = calculateGroupTier(finalPopularity, finalFans);
                updateCharts();

                await interaction.reply(
                    `🎉 ${debutGroup} debuted under ${debutCompany} with "${debutAlbum}" as a **${debutTier}** tier group! Investment: ${debutInvestment.toLocaleString()} :MonthlyPeso:`,
                );
                await saveData();
                break;

            case "comeback":
                const comebackGroup = options.getString("groupname");
                const comebackAlbum = options.getString("albumname");
                const comebackInvestment = options.getInteger("investment");

                if (!botData.groups[comebackGroup]) {
                    await interaction.reply(
                        `❌ Group "${comebackGroup}" not found!`,
                    );
                    return;
                }

                const group = botData.groups[comebackGroup];
                const groupCompany = botData.companies[group.company];

                // Check if user owns the company
                if (groupCompany.owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group.company}"! Only the company owner can manage comebacks.`,
                    );
                    return;
                }

                if (groupCompany.funds < comebackInvestment) {
                    await interaction.reply(
                        `❌ ${group.company} has insufficient funds!`,
                    );
                    return;
                }

                group.albums.push({
                    name: comebackAlbum,
                    investment: comebackInvestment,
                });
                groupCompany.funds -= comebackInvestment;

                // Boost from comeback
                const boost = Math.floor(comebackInvestment / 100);
                group.popularity += boost;
                group.fans += Math.floor(boost * 0.8);
                group.followers += Math.floor(boost * 1.5);

                updateCharts();

                await interaction.reply(
                    `🎵 ${comebackGroup} made a comeback with "${comebackAlbum}"! Investment: ${comebackInvestment.toLocaleString()} :MonthlyPeso:`,
                );
                await saveData();
                break;

            case "setgroup":
                const setGroupName = options.getString("groupname");
                const setCompany = options.getString("company");
                const setDebutAlbum = options.getString("debutalbum");
                const setComebacks = options.getString("comebacks") || "";
                const setTier = options.getString("tier");

                if (botData.groups[setGroupName]) {
                    await interaction.reply(
                        `❌ Group "${setGroupName}" already exists!`,
                    );
                    return;
                }

                if (!botData.companies[setCompany]) {
                    await interaction.reply(
                        `❌ Company "${setCompany}" not found!`,
                    );
                    return;
                }

                // Check if user owns the company
                if (botData.companies[setCompany].owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${setCompany}"! Only the company owner can set groups.`,
                    );
                    return;
                }

                const albums = [{ name: setDebutAlbum, investment: 50000 }];
                if (setComebacks) {
                    setComebacks.split(",").forEach((album) => {
                        albums.push({ name: album.trim(), investment: 30000 });
                    });
                }

                // Generate realistic stats based on tier or company size
                let targetTier = setTier;
                if (!targetTier) {
                    const setCompanySize = botData.companies[setCompany].size;
                    // Default tier based on company size
                    targetTier = setCompanySize === 'small' ? 'small' : 
                                setCompanySize === 'medium' ? 'medium' : 'big';
                }

                const albumMultiplier = albums.length;
                const tierStats = {
                    nugu: { pop: 200, fans: 300, skillRange: [40, 60] },
                    small: { pop: 800, fans: 1500, skillRange: [50, 70] },
                    medium: { pop: 4000, fans: 8000, skillRange: [60, 80] },
                    big: { pop: 15000, fans: 35000, skillRange: [70, 90] },
                    icon: { pop: 60000, fans: 150000, skillRange: [80, 95] },
                };

                const stats = tierStats[targetTier];

                botData.groups[setGroupName] = {
                    name: setGroupName,
                    company: setCompany,
                    popularity: stats.pop * albumMultiplier,
                    fans: stats.fans * albumMultiplier,
                    followers: stats.fans * albumMultiplier * 2,
                    socialMedia: {
                        Titter: Math.floor(stats.fans * albumMultiplier * 1.6),
                        TikTak: Math.floor(stats.fans * albumMultiplier * 2.4),
                        YouTuboo: Math.floor(stats.fans * albumMultiplier * 1.2),
                        Isutagram: Math.floor(stats.fans * albumMultiplier * 2),
                    },
                    albums: albums,
                    skills: {
                        rap: getRandomInt(stats.skillRange[0], stats.skillRange[1]),
                        vocal: getRandomInt(stats.skillRange[0], stats.skillRange[1]),
                        dance: getRandomInt(stats.skillRange[0], stats.skillRange[1]),
                    },
                    wins: Math.floor(albumMultiplier * getRandomInt(1, targetTier === 'icon' ? 10 : targetTier === 'big' ? 7 : 3)),
                    paks: Math.floor(albumMultiplier * (targetTier === 'icon' ? 0.8 : targetTier === 'big' ? 0.3 : 0.1)),
                    keywords: [setGroupName.toLowerCase()],
                };

                botData.companies[setCompany].groups.push(setGroupName);

                await interaction.reply(
                    `✅ ${setGroupName} added to the system as a **${targetTier}** tier group with ${albums.length} albums!`,
                );
                await saveData();
                break;

            case "groups":
                const groupsList = Object.keys(botData.groups);
                if (groupsList.length === 0) {
                    await interaction.reply("No groups found!");
                    return;
                }

                const groupsEmbed = new EmbedBuilder()
                    .setTitle("All Groups")
                    .setColor(0xff69b4)
                    .setDescription(groupsList.join("\n"));

                await interaction.reply({ embeds: [groupsEmbed] });
                break;

            case "groupstats":
                const statsGroupName = options.getString("groupname");
                if (!botData.groups[statsGroupName]) {
                    await interaction.reply(
                        `❌ Group "${statsGroupName}" not found!`,
                    );
                    return;
                }

                const statsGroup = botData.groups[statsGroupName];
                const groupTier = calculateGroupTier(
                    statsGroup.popularity,
                    statsGroup.fans,
                );
                const mentions = botData.mentions[statsGroupName] || 0;

                const socialMediaText = statsGroup.socialMedia ? 
                    `🐦 Titter: ${statsGroup.socialMedia.Titter?.toLocaleString() || 0}\n` +
                    `🎵 TikTak: ${statsGroup.socialMedia.TikTak?.toLocaleString() || 0}\n` +
                    `📺 YouTuboo: ${statsGroup.socialMedia.YouTuboo?.toLocaleString() || 0}\n` +
                    `📸 Isutagram: ${statsGroup.socialMedia.Isutagram?.toLocaleString() || 0}` 
                    : "No social media data";

                const statsEmbed = new EmbedBuilder()
                    .setTitle(`📊 ${statsGroupName} Statistics`)
                    .setColor(0xff69b4)
                    .addFields(
                        {
                            name: "🏢 Company",
                            value: statsGroup.company,
                            inline: true,
                        },
                        {
                            name: "⭐ Tier",
                            value:
                                groupTier.charAt(0).toUpperCase() +
                                groupTier.slice(1),
                            inline: true,
                        },
                        {
                            name: "🔥 Popularity",
                            value: statsGroup.popularity.toLocaleString(),
                            inline: true,
                        },
                        {
                            name: "👥 Fans",
                            value: statsGroup.fans.toLocaleString(),
                            inline: true,
                        },
                        {
                            name: "👤 General Followers",
                            value: statsGroup.followers.toLocaleString(),
                            inline: true,
                        },
                        {
                            name: "💿 Albums",
                            value: statsGroup.albums.length.toString(),
                            inline: true,
                        },
                        {
                            name: "🎭 Skills",
                            value: `Rap: ${statsGroup.skills.rap} | Vocal: ${statsGroup.skills.vocal} | Dance: ${statsGroup.skills.dance}`,
                        },
                        {
                            name: "🏆 Achievements",
                            value: `Wins: ${statsGroup.wins} | PAKs: ${statsGroup.paks}`,
                        },
                        {
                            name: "📱 Social Media Followers",
                            value: socialMediaText,
                            inline: false,
                        },
                        {
                            name: "📈 Weekly Mentions",
                            value: mentions.toString(),
                            inline: true,
                        },
                    );

                await interaction.reply({ embeds: [statsEmbed] });
                break;

            case "addwin":
                const winGroup = options.getString("groupname");
                const musicShow = options.getString("musicshow");

                if (!botData.groups[winGroup]) {
                    await interaction.reply(
                        `❌ Group "${winGroup}" not found!`,
                    );
                    return;
                }

                botData.groups[winGroup].wins++;

                await interaction.reply(
                    `🏆 ${winGroup} won on ${musicShow}! Total wins: ${botData.groups[winGroup].wins}`,
                );
                await saveData();
                break;

            case "resetmentions":
                if (
                    !interaction.member.permissions.has(
                        PermissionFlagsBits.Administrator,
                    )
                ) {
                    await interaction.reply("❌ Admin only command!");
                    return;
                }

                const weeksSinceReset =
                    (Date.now() - botData.lastMentionReset) /
                    (1000 * 60 * 60 * 24 * 7);
                if (weeksSinceReset < 1) {
                    await interaction.reply(
                        "❌ Can only reset mentions once per week!",
                    );
                    return;
                }

                botData.mentions = {};
                botData.lastMentionReset = Date.now();

                await interaction.reply("✅ All mentions have been reset!");
                await saveData();
                break;

            case "brandrep":
                const mentionEntries = Object.entries(botData.mentions);
                mentionEntries.sort((a, b) => b[1] - a[1]);

                const top50 = mentionEntries.slice(0, 50);
                let brandRepText = "";

                top50.forEach((entry, index) => {
                    brandRepText += `${index + 1}. ${entry[0]} - ${entry[1]} mentions\n`;
                });

                const brandRepEmbed = new EmbedBuilder()
                    .setTitle("Brand Reputation Ranking")
                    .setColor(0xff69b4)
                    .setDescription(brandRepText || "No mentions this week");

                await interaction.reply({ embeds: [brandRepEmbed] });
                break;

            case "addkeywords":
                const keywordGroup = options.getString("groupname");
                const keywords = options.getString("keywords");

                if (!botData.groups[keywordGroup]) {
                    await interaction.reply(
                        `❌ Group "${keywordGroup}" not found!`,
                    );
                    return;
                }

                const newKeywords = keywords
                    .split(",")
                    .map((k) => k.trim().toLowerCase());
                botData.groups[keywordGroup].keywords = [
                    ...botData.groups[keywordGroup].keywords,
                    ...newKeywords,
                ];

                await interaction.reply(
                    `✅ Added keywords to ${keywordGroup}: ${newKeywords.join(", ")}`,
                );
                await saveData();
                break;

            case "sales":
                const salesGroup = options.getString("groupname");
                if (!botData.groups[salesGroup]) {
                    await interaction.reply(
                        `❌ Group "${salesGroup}" not found!`,
                    );
                    return;
                }

                const group_sales = botData.groups[salesGroup];
                const tier = calculateGroupTier(
                    group_sales.popularity,
                    group_sales.fans,
                );
                const sales = generateSales(tier, group_sales.popularity);
                const earnings = sales * 2;

                botData.companies[group_sales.company].funds += earnings;

                await interaction.reply(
                    `💿 ${salesGroup} sold ${sales.toLocaleString()} albums! ${group_sales.company} earned ${earnings.toLocaleString()} :MonthlyPeso:`,
                );
                await saveData();
                break;

            case "buzz":
                const buzzGroup = options.getString("groupname");
                if (!botData.groups[buzzGroup]) {
                    await interaction.reply(
                        `❌ Group "${buzzGroup}" not found!`,
                    );
                    return;
                }

                const buzzAmount = getRandomInt(100, 1000);
                const group_buzz = botData.groups[buzzGroup];
                group_buzz.popularity += buzzAmount;
                group_buzz.fans += Math.floor(buzzAmount * 0.8);
                group_buzz.followers += Math.floor(buzzAmount * 1.2);

                await interaction.reply(
                    `📈 ${buzzGroup} gained buzz! +${buzzAmount} popularity, +${Math.floor(buzzAmount * 0.8)} fans, +${Math.floor(buzzAmount * 1.2)} followers`,
                );
                await saveData();
                break;

            case "scandal":
                const scandalGroup = options.getString("groupname");
                if (!botData.groups[scandalGroup]) {
                    await interaction.reply(
                        `❌ Group "${scandalGroup}" not found!`,
                    );
                    return;
                }

                // Give teruakane better luck (80% chance for positive)
                const isPositive = userId === "979346606233104415" ? Math.random() > 0.2 : Math.random() > 0.5;
                const scandalImpact = getRandomInt(500, 2000);
                const group_scandal = botData.groups[scandalGroup];

                const positiveScandals = [
                    "secretly donated millions to charity! 💝",
                    "member was caught helping elderly people cross the street! 👵",
                    "paid for a fan's medical treatment anonymously! 🏥",
                    "member graduated with honors from university! 🎓",
                    "was spotted volunteering at animal shelter! 🐕",
                    "member learned sign language to communicate with deaf fans! 🤟"
                ];

                const negativeScandals = [
                    "dating rumors with another celebrity surfaced! 💔",
                    "member was caught being rude to staff! 😠",
                    "plagiarism accusations for their latest song! 📝",
                    "member made controversial political statements! 🗳️",
                    "tax evasion allegations emerged! 💸",
                    "member was seen smoking in public! 🚬",
                    "accused of attitude problems during filming! 🎬"
                ];

                if (isPositive) {
                    const scandal = positiveScandals[Math.floor(Math.random() * positiveScandals.length)];
                    group_scandal.popularity += scandalImpact;
                    group_scandal.fans += Math.floor(scandalImpact * 0.6);
                    group_scandal.followers += Math.floor(scandalImpact * 0.8);
                    // Boost social media followers too
                    if (group_scandal.socialMedia) {
                        Object.keys(group_scandal.socialMedia).forEach(platform => {
                            group_scandal.socialMedia[platform] += Math.floor(scandalImpact * 0.4);
                        });
                    }
                    await interaction.reply(
                        `✨ BREAKING: ${scandalGroup} ${scandal}\n📈 +${scandalImpact} popularity boost!`,
                    );
                } else {
                    const scandal = negativeScandals[Math.floor(Math.random() * negativeScandals.length)];
                    group_scandal.popularity = Math.max(
                        0,
                        group_scandal.popularity - scandalImpact,
                    );
                    group_scandal.fans = Math.max(
                        0,
                        group_scandal.fans - Math.floor(scandalImpact * 0.6),
                    );
                    group_scandal.followers = Math.max(
                        0,
                        group_scandal.followers -
                            Math.floor(scandalImpact * 0.8),
                    );
                    // Decrease social media followers too
                    if (group_scandal.socialMedia) {
                        Object.keys(group_scandal.socialMedia).forEach(platform => {
                            group_scandal.socialMedia[platform] = Math.max(0, 
                                group_scandal.socialMedia[platform] - Math.floor(scandalImpact * 0.4));
                        });
                    }
                    await interaction.reply(
                        `💥 SCANDAL: ${scandalGroup} ${scandal}\n📉 -${scandalImpact} popularity loss!`,
                    );
                }

                updateCharts();
                await saveData();
                break;

            case "performance":
                const perfGroup = options.getString("groupname");
                if (!botData.groups[perfGroup]) {
                    await interaction.reply(
                        `❌ Group "${perfGroup}" not found!`,
                    );
                    return;
                }

                const group_perf = botData.groups[perfGroup];
                
                // Check if user owns the company
                if (botData.companies[group_perf.company].owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_perf.company}"! Only the company owner can use performance.`,
                    );
                    return;
                }
                const avgSkill =
                    (group_perf.skills.rap +
                        group_perf.skills.vocal +
                        group_perf.skills.dance) /
                    3;

                let performanceLevel;
                let reward;

                // Give teruakane better performance results
                const skillBonus = userId === "979346606233104415" ? 15 : 0;
                const adjustedSkill = avgSkill + skillBonus;

                if (adjustedSkill >= 80) {
                    performanceLevel = "Perfect";
                    reward = { popularity: 800, fans: 600, money: 5000 };
                } else if (adjustedSkill >= 60) {
                    performanceLevel = "Good";
                    reward = { popularity: 400, fans: 300, money: 2000 };
                } else {
                    performanceLevel = "Bad";
                    reward = { popularity: 100, fans: 50, money: 500 };
                }

                // Extra bonus for teruakane
                if (userId === "979346606233104415") {
                    reward.popularity = Math.floor(reward.popularity * 1.3);
                    reward.fans = Math.floor(reward.fans * 1.3);
                    reward.money = Math.floor(reward.money * 1.3);
                }

                group_perf.popularity += reward.popularity;
                group_perf.fans += reward.fans;
                botData.companies[group_perf.company].funds += reward.money;

                await interaction.reply(
                    `🎤 ${perfGroup} had a ${performanceLevel} performance! +${reward.popularity} popularity, +${reward.fans} fans, +${reward.money.toLocaleString()} :MonthlyPeso: to ${group_perf.company}`,
                );
                await saveData();
                break;

            case "trainvocals":
                const vocalGroup = options.getString("groupname");
                if (!botData.groups[vocalGroup]) {
                    await interaction.reply(
                        `❌ Group "${vocalGroup}" not found!`,
                    );
                    return;
                }

                const group_vocal = botData.groups[vocalGroup];
                const company_vocal = botData.companies[group_vocal.company];

                // Check if user owns the company
                if (company_vocal.owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_vocal.company}"! Only the company owner can train groups.`,
                    );
                    return;
                }

                const trainingCost = 5000;

                if (company_vocal.funds < trainingCost) {
                    await interaction.reply(
                        `❌ ${group_vocal.company} has insufficient funds for training!`,
                    );
                    return;
                }

                if (group_vocal.skills.vocal >= 100) {
                    await interaction.reply(
                        `❌ ${vocalGroup}'s vocal skill is already maxed out!`,
                    );
                    return;
                }

                company_vocal.funds -= trainingCost;
                const skillIncrease = userId === "979346606233104415" ? getRandomInt(3, 7) : getRandomInt(1, 5);
                group_vocal.skills.vocal = Math.min(100, group_vocal.skills.vocal + skillIncrease);
                
                // Training also gives small popularity boost
                const trainingBoost = skillIncrease * 10;
                group_vocal.popularity += trainingBoost;
                group_vocal.fans += Math.floor(trainingBoost * 0.5);

                const oldTier = calculateGroupTier(group_vocal.popularity - trainingBoost, group_vocal.fans - Math.floor(trainingBoost * 0.5));
                const newTier = calculateGroupTier(group_vocal.popularity, group_vocal.fans);

                let replyMessage = `🎵 ${vocalGroup} trained vocals! New level: ${group_vocal.skills.vocal}/100 (+${skillIncrease}) | +${trainingBoost} popularity`;
                
                if (oldTier !== newTier) {
                    replyMessage += `\n🔥 **TIER UP!** ${vocalGroup} advanced from ${oldTier} to ${newTier}!`;
                }

                await interaction.reply(replyMessage);
                await saveData();
                break;

            case "traindance":
                const danceGroup = options.getString("groupname");
                if (!botData.groups[danceGroup]) {
                    await interaction.reply(
                        `❌ Group "${danceGroup}" not found!`,
                    );
                    return;
                }

                const group_dance = botData.groups[danceGroup];
                const company_dance = botData.companies[group_dance.company];

                // Check if user owns the company
                if (company_dance.owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_dance.company}"! Only the company owner can train groups.`,
                    );
                    return;
                }

                const danceTrainingCost = 5000;

                if (company_dance.funds < danceTrainingCost) {
                    await interaction.reply(
                        `❌ ${group_dance.company} has insufficient funds for training!`,
                    );
                    return;
                }

                if (group_dance.skills.dance >= 100) {
                    await interaction.reply(
                        `❌ ${danceGroup}'s dance skill is already maxed out!`,
                    );
                    return;
                }

                company_dance.funds -= danceTrainingCost;
                const danceSkillIncrease = userId === "979346606233104415" ? getRandomInt(3, 7) : getRandomInt(1, 5);
                group_dance.skills.dance = Math.min(100, group_dance.skills.dance + danceSkillIncrease);
                
                // Training also gives small popularity boost
                const danceTrainingBoost = danceSkillIncrease * 10;
                group_dance.popularity += danceTrainingBoost;
                group_dance.fans += Math.floor(danceTrainingBoost * 0.5);

                const danceOldTier = calculateGroupTier(group_dance.popularity - danceTrainingBoost, group_dance.fans - Math.floor(danceTrainingBoost * 0.5));
                const danceNewTier = calculateGroupTier(group_dance.popularity, group_dance.fans);

                let danceReplyMessage = `💃 ${danceGroup} trained dance! New level: ${group_dance.skills.dance}/100 (+${danceSkillIncrease}) | +${danceTrainingBoost} popularity`;
                
                if (danceOldTier !== danceNewTier) {
                    danceReplyMessage += `\n🔥 **TIER UP!** ${danceGroup} advanced from ${danceOldTier} to ${danceNewTier}!`;
                }

                await interaction.reply(danceReplyMessage);
                await saveData();
                break;

            case "trainrap":
                const rapGroup = options.getString("groupname");
                if (!botData.groups[rapGroup]) {
                    await interaction.reply(
                        `❌ Group "${rapGroup}" not found!`,
                    );
                    return;
                }

                const group_rap = botData.groups[rapGroup];
                const company_rap = botData.companies[group_rap.company];

                // Check if user owns the company
                if (company_rap.owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_rap.company}"! Only the company owner can train groups.`,
                    );
                    return;
                }

                const rapTrainingCost = 5000;

                if (company_rap.funds < rapTrainingCost) {
                    await interaction.reply(
                        `❌ ${group_rap.company} has insufficient funds for training!`,
                    );
                    return;
                }

                if (group_rap.skills.rap >= 100) {
                    await interaction.reply(
                        `❌ ${rapGroup}'s rap skill is already maxed out!`,
                    );
                    return;
                }

                company_rap.funds -= rapTrainingCost;
                const rapSkillIncrease = userId === "979346606233104415" ? getRandomInt(3, 7) : getRandomInt(1, 5);
                group_rap.skills.rap = Math.min(100, group_rap.skills.rap + rapSkillIncrease);
                
                // Training also gives small popularity boost
                const rapTrainingBoost = rapSkillIncrease * 10;
                group_rap.popularity += rapTrainingBoost;
                group_rap.fans += Math.floor(rapTrainingBoost * 0.5);

                const rapOldTier = calculateGroupTier(group_rap.popularity - rapTrainingBoost, group_rap.fans - Math.floor(rapTrainingBoost * 0.5));
                const rapNewTier = calculateGroupTier(group_rap.popularity, group_rap.fans);

                let rapReplyMessage = `🎤 ${rapGroup} trained rap! New level: ${group_rap.skills.rap}/100 (+${rapSkillIncrease}) | +${rapTrainingBoost} popularity`;
                
                if (rapOldTier !== rapNewTier) {
                    rapReplyMessage += `\n🔥 **TIER UP!** ${rapGroup} advanced from ${rapOldTier} to ${rapNewTier}!`;
                }

                await interaction.reply(rapReplyMessage);
                await saveData();
                break;

            case "sponsorship":
                const sponsorGroup = options.getString("groupname");
                if (!botData.groups[sponsorGroup]) {
                    await interaction.reply(
                        `❌ Group "${sponsorGroup}" not found!`,
                    );
                    return;
                }

                const group_sponsor = botData.groups[sponsorGroup];
                const tier_sponsor = calculateGroupTier(
                    group_sponsor.popularity,
                    group_sponsor.fans,
                );
                let successRate = { small: 0.3, medium: 0.6, big: 0.8, icon: 0.9, nugu: 0.1 }[
                    tier_sponsor
                ];

                // Give teruakane better sponsorship luck
                if (userId === "979346606233104415") {
                    successRate = Math.min(0.95, successRate + 0.3);
                }

                if (Math.random() < successRate) {
                    const sponsorReward = {
                        small: 15000,
                        medium: 50000,
                        big: 150000,
                    }[tier_sponsor];
                    botData.companies[group_sponsor.company].funds +=
                        sponsorReward;
                    group_sponsor.popularity += Math.floor(sponsorReward / 100);

                    await interaction.reply(
                        `🤝 ${sponsorGroup} landed a sponsorship! +${sponsorReward.toLocaleString()} :MonthlyPeso: to ${group_sponsor.company} and +${Math.floor(sponsorReward / 100)} popularity!`,
                    );
                } else {
                    await interaction.reply(
                        `❌ ${sponsorGroup} failed to land a sponsorship this time.`,
                    );
                }

                await saveData();
                break;

            case "stream":
                const streamGroup = options.getString("groupname");
                const streamAlbum = options.getString("albumname");

                // Check cooldown
                const lastStream = botData.users[userId].lastStream || 0;
                const streamNow = Date.now();
                if (streamNow - lastStream < 180000) { // 3 minutes cooldown
                    const remaining = Math.ceil((180000 - (streamNow - lastStream)) / 1000);
                    await interaction.reply(
                        `⏰ You can stream again in ${remaining} seconds!`,
                    );
                    return;
                }

                if (!botData.groups[streamGroup]) {
                    await interaction.reply(
                        `❌ Group "${streamGroup}" not found!`,
                    );
                    return;
                }

                const group_stream = botData.groups[streamGroup];
                const albumExists = group_stream.albums.some(
                    (album) =>
                        album.name.toLowerCase() === streamAlbum.toLowerCase(),
                );

                if (!albumExists) {
                    await interaction.reply(
                        `❌ Album "${streamAlbum}" not found for ${streamGroup}!`,
                    );
                    return;
                }

                const groupTier_stream = calculateGroupTier(group_stream.popularity, group_stream.fans);
                const streamBoosts = {
                    nugu: getRandomInt(10, 50),
                    small: getRandomInt(30, 100),
                    medium: getRandomInt(80, 200),
                    big: getRandomInt(150, 400),
                    icon: getRandomInt(300, 800),
                };

                const streamBoost = streamBoosts[groupTier_stream];
                group_stream.popularity += streamBoost;
                group_stream.fans += Math.floor(streamBoost * 0.3);
                
                botData.users[userId].lastStream = streamNow;

                // Chance to improve chart position
                updateCharts();

                await interaction.reply(
                    `📱 You streamed "${streamAlbum}" by ${streamGroup}! +${streamBoost} popularity and +${Math.floor(streamBoost * 0.3)} fans!`,
                );
                await saveData();
                break;

            case "charts":
                const chartGroup = options.getString("groupname");

                if (chartGroup) {
                    if (!botData.groups[chartGroup]) {
                        await interaction.reply(
                            `❌ Group "${chartGroup}" not found!`,
                        );
                        return;
                    }

                    let groupCharts = "";
                    CHART_PLATFORMS.forEach((platform) => {
                        Object.entries(botData.charts[platform]).forEach(
                            ([song, position]) => {
                                if (song.startsWith(chartGroup)) {
                                    const indicator = getPositionIndicator(song, platform);
                                    groupCharts += `${platform}: #${position} - ${song}${indicator}\n`;
                                }
                            },
                        );
                    });

                    if (!groupCharts) {
                        await interaction.reply(
                            `${chartGroup} is not currently charting.`,
                        );
                        return;
                    }

                    const chartEmbed = new EmbedBuilder()
                        .setTitle(`${chartGroup} Chart Positions`)
                        .setColor(0xff69b4)
                        .setDescription(groupCharts);

                    await interaction.reply({ embeds: [chartEmbed] });
                } else {
                    let allCharts = "";
                    CHART_PLATFORMS.forEach((platform) => {
                        allCharts += `**${platform}**\n`;
                        const sortedEntries = Object.entries(
                            botData.charts[platform],
                        ).sort((a, b) => a[1] - b[1]);
                        sortedEntries
                            .slice(0, 100)
                            .forEach(([song, position]) => {
                                const indicator = getPositionIndicator(song, platform);
                                allCharts += `#${position} - ${song}${indicator}\n`;
                            });
                        allCharts += "\n";
                    });

                    const allChartsEmbed = new EmbedBuilder()
                        .setTitle("📊 Music Charts (TOP 100)")
                        .setColor(0xff69b4)
                        .setDescription(
                            allCharts || "No songs currently charting",
                        );

                    await interaction.reply({ embeds: [allChartsEmbed] });
                }
                break;

            case "payola":
                const payolaGroup = options.getString("groupname");
                const payolaAmount = options.getInteger("amount");

                if (!botData.groups[payolaGroup]) {
                    await interaction.reply(
                        `❌ Group "${payolaGroup}" not found!`,
                    );
                    return;
                }

                if (payolaAmount < 1000000) {
                    await interaction.reply(
                        "❌ Minimum payola amount is 1,000,000 :MonthlyPeso:!",
                    );
                    return;
                }

                const group_payola = botData.groups[payolaGroup];
                const company_payola = botData.companies[group_payola.company];

                // Check if user owns the company
                if (company_payola.owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_payola.company}"! Only the company owner can use payola.`,
                    );
                    return;
                }

                if (company_payola.funds < payolaAmount) {
                    await interaction.reply(
                        `❌ ${group_payola.company} has insufficient funds!`,
                    );
                    return;
                }

                company_payola.funds -= payolaAmount;

                // Improve chart positions
                CHART_PLATFORMS.forEach((platform) => {
                    Object.entries(botData.charts[platform]).forEach(
                        ([song, position]) => {
                            if (song.startsWith(payolaGroup)) {
                                const improvement = Math.floor(
                                    payolaAmount / 10000,
                                );
                                const newPosition = Math.max(
                                    1,
                                    position - improvement,
                                );
                                botData.charts[platform][song] = newPosition;
                            }
                        },
                    );
                });

                // Boost popularity
                const popularityBoost = Math.floor(payolaAmount / 1000);
                group_payola.popularity += popularityBoost;

                // Check for PAK
                if (checkPAK(payolaGroup)) {
                    group_payola.paks++;
                    await interaction.reply(
                        `💰 ${payolaGroup} used payola (${payolaAmount.toLocaleString()} :MonthlyPeso:) and achieved a PAK! 🏆✨ This is a rare and special achievement!`,
                    );
                } else {
                    await interaction.reply(
                        `💰 ${payolaGroup} used payola (${payolaAmount.toLocaleString()} :MonthlyPeso:) to improve chart positions and gain ${popularityBoost.toLocaleString()} popularity!`,
                    );
                }

                await saveData();
                break;

            case "newpost":
                const postGroup = options.getString("groupname");
                const platform = options.getString("platform");

                if (!botData.groups[postGroup]) {
                    await interaction.reply(`❌ Group "${postGroup}" not found!`);
                    return;
                }

                const group_post = botData.groups[postGroup];
                
                // Check if user owns the company
                if (botData.companies[group_post.company].owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_post.company}"! Only the company owner can make posts.`,
                    );
                    return;
                }
                const groupTier_post = calculateGroupTier(group_post.popularity, group_post.fans);

                // Initialize social media if doesn't exist
                if (!group_post.socialMedia) {
                    group_post.socialMedia = {
                        Titter: Math.floor(group_post.followers * 0.8),
                        TikTak: Math.floor(group_post.followers * 1.2),
                        YouTuboo: Math.floor(group_post.followers * 0.6),
                        Isutagram: Math.floor(group_post.followers),
                    };
                }

                // Generate post engagement based on group tier and platform
                const baseEngagement = {
                    small: { likes: [100, 1000], comments: [10, 100] },
                    medium: { likes: [1000, 10000], comments: [100, 1000] },
                    big: { likes: [10000, 100000], comments: [1000, 10000] },
                }[groupTier_post];

                const platformMultiplier = {
                    Titter: 0.8,
                    TikTak: 1.5,
                    YouTuboo: 0.6,
                    Isutagram: 1.2,
                }[platform];

                const likes = Math.floor(getRandomInt(baseEngagement.likes[0], baseEngagement.likes[1]) * platformMultiplier);
                const comments = Math.floor(getRandomInt(baseEngagement.comments[0], baseEngagement.comments[1]) * platformMultiplier);

                // Calculate gains based on engagement
                const fanGain = Math.floor(likes * 0.05 + comments * 0.2);
                const followerGain = Math.floor(likes * 0.03 + comments * 0.15);
                const socialMediaGain = Math.floor(likes * 0.08 + comments * 0.25);

                // Apply gains
                group_post.fans += fanGain;
                group_post.socialMedia[platform] += socialMediaGain;
                group_post.popularity += Math.floor(fanGain * 0.5);

                // Add to mentions for brand reputation
                botData.mentions[postGroup] = (botData.mentions[postGroup] || 0) + Math.floor(comments * 0.1);

                const platformIcons = {
    Titter: "🐦",
    TikTak: "🎵",
    YouTuboo: "📺",
    Isutagram: "📸"
};

// Example usage in your reply
await interaction.reply(
    `${platformIcons[buyPlatform]} ${buyGroup} bought ${buyAmount.toLocaleString()} ${buyPlatform} followers for ${followerCost.toLocaleString()} pesos!\n` +
    `${platformIcons[buyPlatform]} ${buyPlatform}: ${group_buy.socialMedia[buyPlatform].toLocaleString()} followers\n` +
    `👤 General Followers: ${group_buy.followers.toLocaleString()} (+${generalBoost})`
);platformEmojis = {
                    Titter: "🐦",
                    TikTak: "🎵",
                    YouTuboo: "📺",
                    Isutagram: "📸"
                };

                await interaction.reply(
                    `${platformEmojis[platform]} ${postGroup} just made a new ${platform} post!\n` +
                    `❤️ Likes: ${likes.toLocaleString()}\n` +
                    `💬 Comments: ${comments.toLocaleString()}\n` +
                    `👥 Fans: ${group_post.fans.toLocaleString()} (+${fanGain})\n` +
                    `${platformEmojis[platform]} ${platform} Followers: ${group_post.socialMedia[platform].toLocaleString()} (+${socialMediaGain})`
                );

                await saveData();
                break;

            case "companies":
                const companiesList = Object.entries(botData.companies);
                if (companiesList.length === 0) {
                    await interaction.reply("No companies found!");
                    return;
                }

                let companiesText = "";
                companiesList.forEach(([companyName, company]) => {
                    const groupsWithTiers = company.groups.map(groupName => {
                        const group = botData.groups[groupName];
                        if (group) {
                            const tier = calculateGroupTier(group.popularity, group.fans);
                            return `${groupName} (${tier})`;
                        }
                        return groupName;
                    });

                    companiesText += `**${companyName}** (${company.size} company)\n`;
                    companiesText += `📊 Funds: ${company.funds.toLocaleString()} :MonthlyPeso:\n`;
                    companiesText += `👑 Owner: ${company.owner ? `<@${company.owner}>` : 'None'}\n`;
                    companiesText += `🎵 Groups: ${groupsWithTiers.join(", ") || "None"}\n\n`;
                });

                const companiesEmbed = new EmbedBuilder()
                    .setTitle("🏢 All Companies")
                    .setColor(0xff69b4)
                    .setDescription(companiesText);

                await interaction.reply({ embeds: [companiesEmbed] });
                break;

            case "buyfollowers":
                const buyGroup = options.getString("groupname");
                const buyPlatform = options.getString("platform");
                const buyAmount = options.getInteger("amount");

                if (!botData.groups[buyGroup]) {
                    await interaction.reply(`❌ Group "${buyGroup}" not found!`);
                    return;
                }

                if (buyAmount < 1000) {
                    await interaction.reply(
                        "❌ Minimum purchase is 1,000 followers!",
                    );
                    return;
                }

                const group_buy = botData.groups[buyGroup];
                const company_buy = botData.companies[group_buy.company];

                // Check if user owns the company
                if (company_buy.owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_buy.company}"! Only the company owner can buy followers.`,
                    );
                    return;
                }

                // Very expensive: 500 pesos per follower
                const followerCost = buyAmount * 500;

                if (company_buy.funds < followerCost) {
                    await interaction.reply(
                        `❌ ${group_buy.company} has insufficient funds! Cost: ${followerCost.toLocaleString()} :MonthlyPeso:`,
                    );
                    return;
                }

                // Initialize social media if doesn't exist
                if (!group_buy.socialMedia) {
                    group_buy.socialMedia = {
                        Titter: Math.floor(group_buy.followers * 0.8),
                        TikTak: Math.floor(group_buy.followers * 1.2),
                        YouTuboo: Math.floor(group_buy.followers * 0.6),
                        Isutagram: Math.floor(group_buy.followers),
                    };
                }

                company_buy.funds -= followerCost;
                group_buy.socialMedia[buyPlatform] += buyAmount;

                // Small boost to general followers and popularity
                const generalBoost = Math.floor(buyAmount * 0.1);
                group_buy.followers += generalBoost;
                group_buy.popularity += Math.floor(generalBoost * 0.5);

                const platformEmojis = {
                    Titter: "🐦",
                    TikTak: "🎵",
                    YouTuboo: "📺",
                    Isutagram: "📸"
                };

                await interaction.reply(
                    `${platformEmojis[buyPlatform]} ${buyGroup} bought ${buyAmount.toLocaleString()} ${buyPlatform} followers for ${followerCost.toLocaleString()} :MonthlyPeso:!\n` +
                    `${platformEmojis[buyPlatform]} ${buyPlatform}: ${group_buy.socialMedia[buyPlatform].toLocaleString()} followers\n` +
                    `👤 General Followers: ${group_buy.followers.toLocaleString()} (+${generalBoost})`
                );

                await saveData();
                break;

            default:
                await interaction.reply("❌ Unknown command!");
        }
    } catch (error) {
        console.error("Command error:", error);
        await interaction.reply(
            "❌ An error occurred while processing the command.",
        );
    }
});

// Message listener for mentions
client.on("messageCreate", (message) => {
    if (message.author.bot) return;

    const content = message.content.toLowerCase();

    Object.values(botData.groups).forEach((group) => {
        if (group.keywords) {
            group.keywords.forEach((keyword) => {
                if (content.includes(keyword)) {
                    botData.mentions[group.name] =
                        (botData.mentions[group.name] || 0) + 1;
                }
            });
        }
    });

    saveData();
});

// Bot startup
client.once("ready", async () => {
    console.log(`${client.user.tag} is online!`);

    await loadData();

    // Register slash commands
    try {
        console.log("Registering slash commands...");
        await client.application.commands.set(commands);
        console.log("Slash commands registered successfully!");
    } catch (error) {
        console.error("Error registering commands:", error);
    }

    // Auto-save every 5 minutes
    setInterval(saveData, 300000);

    // Update charts every hour
    setInterval(updateCharts, 3600000);
});

// Error handling
client.on("error", (error) => {
    console.error("Discord client error:", error);
});

process.on("unhandledRejection", (error) => {
    console.error("Unhandled promise rejection:", error);
});

// Login with bot token
client.login(process.env.BOT_TOKEN);
